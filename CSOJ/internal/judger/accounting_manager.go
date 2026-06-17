package judger

import (
	"fmt"
	"strings"

	"github.com/ZJUSCT/CSOJ/internal/config"
)

type AssociationRecord struct {
	Account       string  `json:"account"`
	ParentAccount string  `json:"parent_account"`
	User          string  `json:"user"`
	QOS           string  `json:"qos"`
	Fairshare     int     `json:"fairshare"`
	MaxJobs       int     `json:"max_jobs"`
	MaxSubmit     int     `json:"max_submit"`
	MaxBillingRun float64 `json:"max_billing_running"`
	MaxBillingSub float64 `json:"max_billing_submit"`
}

type AssociationUpdate struct {
	Account       string
	ParentAccount *string
	User          string
	QOS           string
	Fairshare     *int
	MaxJobs       *int
	MaxSubmit     *int
	MaxBillingRun *float64
	MaxBillingSub *float64
}

type SchedulerConfigSnapshot struct {
	QueueSize       int
	Backfill        bool
	PriorityWeights config.PriorityWeights
	BillingWeights  map[string]float64
	FairshareDecay  config.FairshareDecay
	Accounts        []config.Account
	QOS             []config.QOS
	Reservations    []config.Reservation
}

func (s *Scheduler) GetSchedulerConfigSnapshot() SchedulerConfigSnapshot {
	s.configMu.RLock()
	defer s.configMu.RUnlock()

	accounts := make([]config.Account, 0, len(s.cfg.Scheduler.Accounts))
	for _, account := range s.cfg.Scheduler.Accounts {
		accounts = append(accounts, cloneAccount(account))
	}
	qosItems := make([]config.QOS, 0, len(s.cfg.Scheduler.QOS))
	for _, qos := range s.cfg.Scheduler.QOS {
		qosItems = append(qosItems, cloneQOS(qos))
	}
	reservations := make([]config.Reservation, 0, len(s.cfg.Scheduler.Reservations))
	for _, reservation := range s.cfg.Scheduler.Reservations {
		reservations = append(reservations, cloneReservation(reservation))
	}
	backfill := false
	if s.cfg.Scheduler.Backfill != nil {
		backfill = *s.cfg.Scheduler.Backfill
	}
	queueSize := s.cfg.Scheduler.QueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}
	billingWeights := make(map[string]float64, len(s.cfg.Scheduler.BillingWeights))
	for name, weight := range s.cfg.Scheduler.BillingWeights {
		billingWeights[name] = weight
	}

	return SchedulerConfigSnapshot{
		QueueSize:       queueSize,
		Backfill:        backfill,
		PriorityWeights: s.cfg.Scheduler.PriorityWeights,
		BillingWeights:  billingWeights,
		FairshareDecay:  s.cfg.Scheduler.FairshareDecay,
		Accounts:        accounts,
		QOS:             qosItems,
		Reservations:    reservations,
	}
}

func (s *Scheduler) ListAccounts(name string) []config.Account {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	accounts := make([]config.Account, 0, len(s.cfg.Scheduler.Accounts))
	for _, account := range s.cfg.Scheduler.Accounts {
		if name != "" && !strings.EqualFold(account.Name, name) {
			continue
		}
		accounts = append(accounts, cloneAccount(account))
	}
	return accounts
}

func (s *Scheduler) UpsertAccount(account config.Account) (config.Account, error) {
	account.Name = strings.TrimSpace(account.Name)
	if account.Name == "" {
		return config.Account{}, fmt.Errorf("account name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Accounts {
		if strings.EqualFold(s.cfg.Scheduler.Accounts[i].Name, account.Name) {
			s.cfg.Scheduler.Accounts[i] = cloneAccount(account)
			return cloneAccount(account), nil
		}
	}
	s.cfg.Scheduler.Accounts = append(s.cfg.Scheduler.Accounts, cloneAccount(account))
	return cloneAccount(account), nil
}

func (s *Scheduler) DeleteAccount(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("account name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Accounts {
		if strings.EqualFold(s.cfg.Scheduler.Accounts[i].Name, name) {
			s.cfg.Scheduler.Accounts = append(s.cfg.Scheduler.Accounts[:i], s.cfg.Scheduler.Accounts[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("account %q not found", name)
}

func (s *Scheduler) ListQOS(name string) []config.QOS {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	qosItems := make([]config.QOS, 0, len(s.cfg.Scheduler.QOS))
	for _, qos := range s.cfg.Scheduler.QOS {
		if name != "" && !strings.EqualFold(qos.Name, name) {
			continue
		}
		qosItems = append(qosItems, cloneQOS(qos))
	}
	return qosItems
}

func (s *Scheduler) UpsertQOS(qos config.QOS) (config.QOS, error) {
	qos.Name = strings.TrimSpace(qos.Name)
	if qos.Name == "" {
		return config.QOS{}, fmt.Errorf("qos name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.QOS {
		if strings.EqualFold(s.cfg.Scheduler.QOS[i].Name, qos.Name) {
			s.cfg.Scheduler.QOS[i] = cloneQOS(qos)
			return cloneQOS(qos), nil
		}
	}
	s.cfg.Scheduler.QOS = append(s.cfg.Scheduler.QOS, cloneQOS(qos))
	return cloneQOS(qos), nil
}

func (s *Scheduler) DeleteQOS(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("qos name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.QOS {
		if strings.EqualFold(s.cfg.Scheduler.QOS[i].Name, name) {
			s.cfg.Scheduler.QOS = append(s.cfg.Scheduler.QOS[:i], s.cfg.Scheduler.QOS[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("qos %q not found", name)
}

func (s *Scheduler) ListAssociations(accountFilter, userFilter, qosFilter string) []AssociationRecord {
	s.configMu.RLock()
	accounts := make([]config.Account, 0, len(s.cfg.Scheduler.Accounts))
	for _, account := range s.cfg.Scheduler.Accounts {
		accounts = append(accounts, cloneAccount(account))
	}
	s.configMu.RUnlock()

	records := make([]AssociationRecord, 0)
	for _, account := range accounts {
		if accountFilter != "" && !strings.EqualFold(account.Name, accountFilter) {
			continue
		}
		users := account.Users
		if len(users) == 0 {
			users = []string{"*"}
		}
		qosItems := account.AllowQOS
		if len(qosItems) == 0 {
			qosItems = []string{"*"}
		}
		for _, user := range users {
			if userFilter != "" && !strings.EqualFold(user, userFilter) {
				continue
			}
			for _, qos := range qosItems {
				if qosFilter != "" && !strings.EqualFold(qos, qosFilter) {
					continue
				}
				records = append(records, AssociationRecord{
					Account:       account.Name,
					ParentAccount: account.ParentName,
					User:          user,
					QOS:           qos,
					Fairshare:     account.Fairshare,
					MaxJobs:       account.MaxJobs,
					MaxSubmit:     account.MaxSubmit,
					MaxBillingRun: account.MaxBillingRunning,
					MaxBillingSub: account.MaxBillingSubmit,
				})
			}
		}
	}
	return records
}

func (s *Scheduler) UpsertAssociation(update AssociationUpdate) (AssociationRecord, error) {
	update.Account = strings.TrimSpace(update.Account)
	update.User = strings.TrimSpace(update.User)
	update.QOS = strings.TrimSpace(update.QOS)
	if update.Account == "" {
		return AssociationRecord{}, fmt.Errorf("account is required")
	}

	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Accounts {
		if !strings.EqualFold(s.cfg.Scheduler.Accounts[i].Name, update.Account) {
			continue
		}
		account := cloneAccount(s.cfg.Scheduler.Accounts[i])
		if update.ParentAccount != nil {
			account.ParentName = strings.TrimSpace(*update.ParentAccount)
		}
		if update.User != "" {
			if update.User == "*" {
				account.Users = nil
			} else {
				account.Users = appendUniqueFold(account.Users, update.User)
			}
		}
		if update.QOS != "" {
			if update.QOS == "*" {
				account.AllowQOS = nil
			} else {
				account.AllowQOS = appendUniqueFold(account.AllowQOS, update.QOS)
			}
		}
		if update.Fairshare != nil {
			account.Fairshare = *update.Fairshare
		}
		if update.MaxJobs != nil {
			account.MaxJobs = *update.MaxJobs
		}
		if update.MaxSubmit != nil {
			account.MaxSubmit = *update.MaxSubmit
		}
		if update.MaxBillingRun != nil {
			account.MaxBillingRunning = *update.MaxBillingRun
		}
		if update.MaxBillingSub != nil {
			account.MaxBillingSubmit = *update.MaxBillingSub
		}
		s.cfg.Scheduler.Accounts[i] = cloneAccount(account)
		return associationFromAccount(account, update.User, update.QOS), nil
	}
	return AssociationRecord{}, fmt.Errorf("account %q not found", update.Account)
}

func (s *Scheduler) DeleteAssociation(accountName, user, qos string) (AssociationRecord, error) {
	accountName = strings.TrimSpace(accountName)
	user = strings.TrimSpace(user)
	qos = strings.TrimSpace(qos)
	if accountName == "" {
		return AssociationRecord{}, fmt.Errorf("account is required")
	}
	if user == "" && qos == "" {
		return AssociationRecord{}, fmt.Errorf("user or qos is required")
	}

	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Accounts {
		if !strings.EqualFold(s.cfg.Scheduler.Accounts[i].Name, accountName) {
			continue
		}
		account := cloneAccount(s.cfg.Scheduler.Accounts[i])
		if user != "" && user != "*" {
			if len(account.Users) == 0 {
				return AssociationRecord{}, fmt.Errorf("account %q uses wildcard users", accountName)
			}
			next, removed := removeStringFold(account.Users, user)
			if !removed {
				return AssociationRecord{}, fmt.Errorf("user %q is not associated with account %q", user, accountName)
			}
			if len(next) == 0 {
				return AssociationRecord{}, fmt.Errorf("cannot delete the last user association from account %q", accountName)
			}
			account.Users = next
		}
		if qos != "" && qos != "*" {
			if len(account.AllowQOS) == 0 {
				return AssociationRecord{}, fmt.Errorf("account %q uses wildcard qos", accountName)
			}
			next, removed := removeStringFold(account.AllowQOS, qos)
			if !removed {
				return AssociationRecord{}, fmt.Errorf("qos %q is not associated with account %q", qos, accountName)
			}
			if len(next) == 0 {
				return AssociationRecord{}, fmt.Errorf("cannot delete the last qos association from account %q", accountName)
			}
			account.AllowQOS = next
		}
		s.cfg.Scheduler.Accounts[i] = cloneAccount(account)
		return associationFromAccount(account, user, qos), nil
	}
	return AssociationRecord{}, fmt.Errorf("account %q not found", accountName)
}

func (s *Scheduler) ListReservations(name string) []config.Reservation {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	reservations := make([]config.Reservation, 0, len(s.cfg.Scheduler.Reservations))
	for _, reservation := range s.cfg.Scheduler.Reservations {
		if name != "" && !strings.EqualFold(reservation.Name, name) {
			continue
		}
		reservations = append(reservations, cloneReservation(reservation))
	}
	return reservations
}

func (s *Scheduler) UpsertReservation(reservation config.Reservation) (config.Reservation, error) {
	reservation.Name = strings.TrimSpace(reservation.Name)
	if reservation.Name == "" {
		return config.Reservation{}, fmt.Errorf("reservation name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Reservations {
		if strings.EqualFold(s.cfg.Scheduler.Reservations[i].Name, reservation.Name) {
			s.cfg.Scheduler.Reservations[i] = cloneReservation(reservation)
			return cloneReservation(reservation), nil
		}
	}
	s.cfg.Scheduler.Reservations = append(s.cfg.Scheduler.Reservations, cloneReservation(reservation))
	return cloneReservation(reservation), nil
}

func (s *Scheduler) DeleteReservation(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("reservation name is required")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	for i := range s.cfg.Scheduler.Reservations {
		if strings.EqualFold(s.cfg.Scheduler.Reservations[i].Name, name) {
			s.cfg.Scheduler.Reservations = append(s.cfg.Scheduler.Reservations[:i], s.cfg.Scheduler.Reservations[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("reservation %q not found", name)
}

func cloneAccount(account config.Account) config.Account {
	account.Users = append([]string(nil), account.Users...)
	account.AllowQOS = append([]string(nil), account.AllowQOS...)
	return account
}

func associationFromAccount(account config.Account, user, qos string) AssociationRecord {
	if user == "" {
		user = "*"
		if len(account.Users) > 0 {
			user = account.Users[0]
		}
	}
	if qos == "" {
		qos = "*"
		if len(account.AllowQOS) > 0 {
			qos = account.AllowQOS[0]
		}
	}
	return AssociationRecord{
		Account:       account.Name,
		ParentAccount: account.ParentName,
		User:          user,
		QOS:           qos,
		Fairshare:     account.Fairshare,
		MaxJobs:       account.MaxJobs,
		MaxSubmit:     account.MaxSubmit,
		MaxBillingRun: account.MaxBillingRunning,
		MaxBillingSub: account.MaxBillingSubmit,
	}
}

func appendUniqueFold(values []string, value string) []string {
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}

func removeStringFold(values []string, value string) ([]string, bool) {
	next := make([]string, 0, len(values))
	removed := false
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			removed = true
			continue
		}
		next = append(next, existing)
	}
	return next, removed
}

func cloneQOS(qos config.QOS) config.QOS {
	qos.Preempt = append([]string(nil), qos.Preempt...)
	return qos
}

func cloneReservation(reservation config.Reservation) config.Reservation {
	reservation.Nodes = append([]string(nil), reservation.Nodes...)
	reservation.Users = append([]string(nil), reservation.Users...)
	reservation.Accounts = append([]string(nil), reservation.Accounts...)
	return reservation
}

func cloneClusterConfig(cluster config.Cluster) config.Cluster {
	cluster.AllowUsers = append([]string(nil), cluster.AllowUsers...)
	cluster.AllowAccounts = append([]string(nil), cluster.AllowAccounts...)
	cluster.AllowQOS = append([]string(nil), cluster.AllowQOS...)
	cluster.DenyQOS = append([]string(nil), cluster.DenyQOS...)
	cluster.Nodes = append([]config.Node(nil), cluster.Nodes...)
	for i := range cluster.Nodes {
		cluster.Nodes[i].Features = append([]string(nil), cluster.Nodes[i].Features...)
		cluster.Nodes[i].GRES = append([]string(nil), cluster.Nodes[i].GRES...)
		cluster.Nodes[i].Kubernetes.ImagePullSecrets = append([]string(nil), cluster.Nodes[i].Kubernetes.ImagePullSecrets...)
		cluster.Nodes[i].Kubernetes.Tolerations = append([]config.KubernetesToleration(nil), cluster.Nodes[i].Kubernetes.Tolerations...)
		cluster.Nodes[i].Kubernetes.RunnerCommand = append([]string(nil), cluster.Nodes[i].Kubernetes.RunnerCommand...)
		if cluster.Nodes[i].Kubernetes.NodeSelector != nil {
			selector := make(map[string]string, len(cluster.Nodes[i].Kubernetes.NodeSelector))
			for key, value := range cluster.Nodes[i].Kubernetes.NodeSelector {
				selector[key] = value
			}
			cluster.Nodes[i].Kubernetes.NodeSelector = selector
		}
	}
	return cluster
}
