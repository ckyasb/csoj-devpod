package judger

import (
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
	"go.uber.org/zap"
)

type SlurmMailEvent string

const (
	SlurmMailBegin     SlurmMailEvent = "BEGIN"
	SlurmMailEnd       SlurmMailEvent = "END"
	SlurmMailFail      SlurmMailEvent = "FAIL"
	SlurmMailRequeue   SlurmMailEvent = "REQUEUE"
	SlurmMailTimeLimit SlurmMailEvent = "TIME_LIMIT"
)

type SlurmMailMessage struct {
	To      []string
	Subject string
	Body    string
	Event   SlurmMailEvent
	JobID   string
}

type MailSender interface {
	Send(SlurmMailMessage) error
}

type noopMailSender struct{}

func (noopMailSender) Send(SlurmMailMessage) error { return nil }

type smtpMailSender struct {
	cfg config.Mail
}

func NewMailSender(cfg config.Mail) MailSender {
	if !cfg.Enabled {
		return noopMailSender{}
	}
	return &smtpMailSender{cfg: cfg}
}

func (s *smtpMailSender) Send(message SlurmMailMessage) error {
	if s == nil || !s.cfg.Enabled {
		return nil
	}
	if len(message.To) == 0 {
		return nil
	}
	if strings.TrimSpace(s.cfg.Host) == "" {
		return fmt.Errorf("mail host is required")
	}
	port := s.cfg.Port
	if port <= 0 {
		port = 25
	}
	from := strings.TrimSpace(s.cfg.From)
	if from == "" {
		from = strings.TrimSpace(s.cfg.Username)
	}
	if from == "" {
		from = "csoj@localhost"
	}

	addr := net.JoinHostPort(s.cfg.Host, fmt.Sprint(port))
	var auth smtp.Auth
	if s.cfg.Username != "" {
		auth = smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
	}
	data := smtpMessage(from, message.To, message.Subject, message.Body)
	return smtp.SendMail(addr, auth, from, message.To, []byte(data))
}

func (s *Scheduler) SetMailSender(sender MailSender) {
	if sender == nil {
		s.mailSender = NewMailSender(s.cfg.Mail)
		return
	}
	s.mailSender = sender
}

func (s *Scheduler) notifySubmissionMail(sub *models.Submission, event SlurmMailEvent, detail string) {
	if s == nil || s.mailSender == nil || sub == nil {
		return
	}
	if !submissionMailTypeMatches(sub.MailType, event) {
		return
	}
	recipients := parseMailRecipients(sub.MailUser)
	if len(recipients) == 0 {
		return
	}
	message := buildSlurmMailMessage(sub, event, detail, recipients)
	if err := s.mailSender.Send(message); err != nil {
		zap.S().Warnf("failed to send %s mail for submission %s: %v", event, sub.ID, err)
	}
}

func submissionMailTypeMatches(mailType string, event SlurmMailEvent) bool {
	tokens := slurmMailTypeTokens(mailType)
	if len(tokens) == 0 || tokens["NONE"] {
		return false
	}
	if tokens["ALL"] {
		return true
	}
	eventToken := normalizeSlurmMailToken(string(event))
	if tokens[eventToken] {
		return true
	}
	if event == SlurmMailTimeLimit && tokens["FAIL"] {
		return true
	}
	return false
}

func slurmMailTypeTokens(mailType string) map[string]bool {
	tokens := make(map[string]bool)
	for _, raw := range strings.FieldsFunc(mailType, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	}) {
		token := normalizeSlurmMailToken(raw)
		if token != "" {
			tokens[token] = true
		}
	}
	return tokens
}

func normalizeSlurmMailToken(token string) string {
	token = strings.ToUpper(strings.TrimSpace(token))
	token = strings.ReplaceAll(token, "-", "")
	token = strings.ReplaceAll(token, "_", "")
	return token
}

func parseMailRecipients(mailUser string) []string {
	var recipients []string
	for _, raw := range strings.FieldsFunc(mailUser, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	}) {
		if recipient := strings.TrimSpace(raw); recipient != "" {
			recipients = append(recipients, recipient)
		}
	}
	return recipients
}

func buildSlurmMailMessage(sub *models.Submission, event SlurmMailEvent, detail string, to []string) SlurmMailMessage {
	state, reason := models.DeriveSlurmJobState(sub.Status, sub.Hold, sub.Reason)
	if detail == "" {
		detail = firstNonEmptyString(reason, sub.Reason)
	}
	jobName := firstNonEmptyString(sub.JobName, sub.ProblemID, sub.ID)
	subject := fmt.Sprintf("CSOJ job %s %s", sub.ID, event)
	body := strings.Join([]string{
		"CSOJ Slurm-compatible job notification",
		"",
		"JobID: " + sub.ID,
		"JobName: " + jobName,
		"UserID: " + sub.UserID,
		"ProblemID: " + sub.ProblemID,
		"Partition: " + sub.Cluster,
		"Node: " + sub.Node,
		"Event: " + string(event),
		"State: " + state,
		"Reason: " + detail,
		"Time: " + time.Now().Format(time.RFC3339),
	}, "\n")
	return SlurmMailMessage{
		To:      append([]string(nil), to...),
		Subject: subject,
		Body:    body,
		Event:   event,
		JobID:   sub.ID,
	}
}

func smtpMessage(from string, to []string, subject, body string) string {
	headers := []string{
		"From: " + sanitizeMailHeader(from),
		"To: " + sanitizeMailHeader(strings.Join(to, ", ")),
		"Subject: " + sanitizeMailHeader(subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
	}
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + body + "\r\n"
}

func sanitizeMailHeader(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func slurmMailEventForFailure(reason string) SlurmMailEvent {
	if normalizeSlurmMailToken(reason) == "TIMELIMIT" || strings.Contains(normalizeSlurmMailToken(reason), "TIMELIMIT") {
		return SlurmMailTimeLimit
	}
	return SlurmMailFail
}
