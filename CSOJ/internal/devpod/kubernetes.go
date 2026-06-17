package devpod

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func (m *Manager) SyncUser(ctx context.Context, user models.User, keys []models.UserSSHKey) error {
	owner := DevPodOwnerName(user)
	if len(keys) == 0 {
		_, err := m.run(ctx, nil, false, "delete", "users.devpod.io", owner, "--ignore-not-found=true")
		return err
	}
	manifest := m.BuildUserManifest(user, keys)
	data, err := marshalYAMLDocuments(manifest)
	if err != nil {
		return err
	}
	_, err = m.run(ctx, data, false, "apply", "-f", "-")
	return err
}

func (m *Manager) CreateDevPod(ctx context.Context, session *models.DevPodSession, plan CreatePlan) error {
	objects := []map[string]interface{}{
		m.BuildDevPodManifest(session, plan),
		m.BuildDefaultDenyNetworkPolicy(),
		m.BuildAllowNetworkPolicy(session, plan),
	}
	data, err := marshalYAMLDocuments(objects...)
	if err != nil {
		return err
	}
	_, err = m.run(ctx, data, true, "apply", "-f", "-")
	return err
}

func (m *Manager) SetRunning(ctx context.Context, session *models.DevPodSession, running bool) error {
	patch := map[string]interface{}{
		"spec": map[string]interface{}{"running": running},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = m.run(ctx, nil, true, "patch", "devpods.devpod.io", session.K8sResourceName, "--type", "merge", "-p", string(data))
	return err
}

func (m *Manager) DeleteDevPod(ctx context.Context, session *models.DevPodSession) error {
	if _, err := m.run(ctx, nil, true, "delete", "devpods.devpod.io", session.K8sResourceName, "--ignore-not-found=true"); err != nil {
		return err
	}
	_, err := m.run(ctx, nil, true, "delete", "networkpolicy", allowNetworkPolicyName(session), "--ignore-not-found=true")
	return err
}

func (m *Manager) RefreshStatus(ctx context.Context, session *models.DevPodSession) error {
	remote, running, err := m.RemoteStatus(ctx, session)
	if err != nil {
		return err
	}
	session.Status = statusFromPhase(remote.Phase, running)
	if remote.LastActivityTime != nil {
		session.LastActivityAt = remote.LastActivityTime
	}
	return nil
}

func (m *Manager) RemoteStatus(ctx context.Context, session *models.DevPodSession) (RemoteStatus, bool, error) {
	output, err := m.run(ctx, nil, true, "get", "devpods.devpod.io", session.K8sResourceName, "-o", "json")
	if err != nil {
		return RemoteStatus{}, false, err
	}
	var obj struct {
		Spec struct {
			Running bool `json:"running"`
		} `json:"spec"`
		Status struct {
			Phase            string `json:"phase"`
			Endpoint         string `json:"endpoint"`
			LastActivityTime string `json:"lastActivityTime"`
			WorkloadRef      struct {
				Name string `json:"name"`
			} `json:"workloadRef"`
		} `json:"status"`
	}
	if err := json.Unmarshal(output, &obj); err != nil {
		return RemoteStatus{}, false, err
	}
	status := RemoteStatus{
		Phase:        obj.Status.Phase,
		Endpoint:     obj.Status.Endpoint,
		WorkloadName: obj.Status.WorkloadRef.Name,
	}
	if obj.Status.LastActivityTime != "" {
		if parsed, err := time.Parse(time.RFC3339, obj.Status.LastActivityTime); err == nil {
			status.LastActivityTime = &parsed
		}
	}
	return status, obj.Spec.Running, nil
}

func (m *Manager) Logs(ctx context.Context, session *models.DevPodSession) (string, error) {
	remote, _, err := m.RemoteStatus(ctx, session)
	if err != nil {
		return "", err
	}
	if remote.WorkloadName == "" {
		return "", fmt.Errorf("DevPod workload is not ready yet")
	}
	output, err := m.run(ctx, nil, true, "logs", "pod/"+remote.WorkloadName, "-c", "workspace", "--tail=300")
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (m *Manager) BuildUserManifest(user models.User, keys []models.UserSSHKey) map[string]interface{} {
	pubkeys := make([]string, 0, len(keys))
	for _, key := range keys {
		pubkeys = append(pubkeys, key.PublicKey)
	}
	displayName := user.Nickname
	if displayName == "" {
		displayName = user.Username
	}
	return map[string]interface{}{
		"apiVersion": "devpod.io/v1alpha1",
		"kind":       "User",
		"metadata": map[string]interface{}{
			"name": DevPodOwnerName(user),
			"labels": map[string]string{
				"app.kubernetes.io/managed-by": "csoj",
				"csoj.zjusct.io/user-id":       labelValue(user.ID),
			},
		},
		"spec": map[string]interface{}{
			"pubkeys":     pubkeys,
			"displayName": displayName,
			"oidcSubject": user.ID,
		},
	}
}

func (m *Manager) BuildDevPodManifest(session *models.DevPodSession, plan CreatePlan) map[string]interface{} {
	labels := devPodLabels(session)
	annotations := map[string]string{
		"csoj.zjusct.io/allow-internet": fmt.Sprint(plan.NetworkProfile.AllowInternet),
		"csoj.zjusct.io/host-network":   fmt.Sprint(session.HostNetwork),
		"csoj.zjusct.io/mpi-enabled":    fmt.Sprint(session.MPIEnabled),
		"csoj.zjusct.io/expires-at":     session.ExpiresAt.Format(time.RFC3339),
	}
	podSpec := m.buildPodSpec(session, plan)
	spec := map[string]interface{}{
		"owner":              session.OwnerName,
		"running":            true,
		"idleTimeoutSeconds": session.IdleTimeoutSeconds,
		"shell":              m.cfg.Defaults.Shell,
		"pod": map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels":      labels,
				"annotations": annotations,
			},
			"spec": podSpec,
		},
	}
	if session.Persistent {
		persistence := map[string]interface{}{
			"size":            formatQuantityGB(session.StorageGB),
			"mountPath":       m.cfg.Defaults.MountPath,
			"targetContainer": "workspace",
		}
		if m.cfg.StorageClassName != "" {
			persistence["storageClassName"] = m.cfg.StorageClassName
		}
		spec["persistence"] = persistence
	}
	return map[string]interface{}{
		"apiVersion": "devpod.io/v1alpha1",
		"kind":       "DevPod",
		"metadata": map[string]interface{}{
			"name":        session.K8sResourceName,
			"namespace":   m.cfg.Namespace,
			"labels":      labels,
			"annotations": annotations,
		},
		"spec": spec,
	}
}

func (m *Manager) buildPodSpec(session *models.DevPodSession, plan CreatePlan) map[string]interface{} {
	resources := map[string]interface{}{
		"requests": map[string]string{
			"cpu":    fmt.Sprintf("%d", session.CPU),
			"memory": formatQuantityMB(session.MemoryMB),
		},
		"limits": map[string]string{
			"cpu":    fmt.Sprintf("%d", session.CPU),
			"memory": formatQuantityMB(session.MemoryMB),
		},
	}
	if session.GPU > 0 {
		resources["limits"].(map[string]string)["nvidia.com/gpu"] = fmt.Sprintf("%d", session.GPU)
		resources["requests"].(map[string]string)["nvidia.com/gpu"] = fmt.Sprintf("%d", session.GPU)
	}
	container := map[string]interface{}{
		"name":      "workspace",
		"image":     session.Image,
		"command":   plan.Command,
		"env":       sortedEnv(plan.Env),
		"resources": resources,
		"securityContext": map[string]interface{}{
			"allowPrivilegeEscalation": false,
			"capabilities": map[string][]string{
				"drop": []string{"ALL"},
			},
			"runAsNonRoot": true,
			"runAsUser":    1000,
			"runAsGroup":   1000,
		},
	}
	podSpec := map[string]interface{}{
		"automountServiceAccountToken": false,
		"enableServiceLinks":           false,
		"hostIPC":                      false,
		"hostPID":                      false,
		"containers":                   []map[string]interface{}{container},
		"restartPolicy":                "Always",
	}
	if session.HostNetwork {
		podSpec["hostNetwork"] = true
		podSpec["dnsPolicy"] = "ClusterFirstWithHostNet"
	} else {
		podSpec["hostNetwork"] = false
	}
	if m.cfg.ServiceAccount != "" {
		podSpec["serviceAccountName"] = m.cfg.ServiceAccount
	}
	nodeSelector := m.nodeSelectorFor(plan.NetworkProfile, session.HostNetwork)
	if len(nodeSelector) > 0 {
		podSpec["nodeSelector"] = nodeSelector
	}
	tolerations := tolerationMaps(append(m.cfg.Tolerations, plan.NetworkProfile.Tolerations...))
	if len(tolerations) > 0 {
		podSpec["tolerations"] = tolerations
	}
	if len(m.cfg.ImagePullSecrets) > 0 {
		refs := make([]map[string]string, 0, len(m.cfg.ImagePullSecrets))
		for _, name := range m.cfg.ImagePullSecrets {
			if strings.TrimSpace(name) != "" {
				refs = append(refs, map[string]string{"name": name})
			}
		}
		podSpec["imagePullSecrets"] = refs
	}
	if m.cfg.PriorityClassName != "" {
		podSpec["priorityClassName"] = m.cfg.PriorityClassName
	}
	if m.cfg.RuntimeClassName != "" {
		podSpec["runtimeClassName"] = m.cfg.RuntimeClassName
	}
	return podSpec
}

func (m *Manager) BuildDefaultDenyNetworkPolicy() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      "csoj-devpod-default-deny",
			"namespace": m.cfg.Namespace,
			"labels": map[string]string{
				"app.kubernetes.io/managed-by": "csoj",
			},
		},
		"spec": map[string]interface{}{
			"podSelector": map[string]interface{}{
				"matchLabels": map[string]string{"app": "csoj-devpod"},
			},
			"policyTypes": []string{"Ingress", "Egress"},
		},
	}
}

func (m *Manager) BuildAllowNetworkPolicy(session *models.DevPodSession, plan CreatePlan) map[string]interface{} {
	backendPort := m.cfg.Gateway.BackendPort
	selector := map[string]string{
		"app":                         "csoj-devpod",
		"csoj.zjusct.io/session-id":   labelValue(session.ID),
		"csoj.zjusct.io/k8s-resource": labelValue(session.K8sResourceName),
	}
	ingress := []map[string]interface{}{
		{
			"from": []map[string]interface{}{
				{
					"namespaceSelector": map[string]interface{}{
						"matchLabels": map[string]string{"kubernetes.io/metadata.name": m.cfg.Gateway.Namespace},
					},
					"podSelector": map[string]interface{}{
						"matchLabels": map[string]string{"app.kubernetes.io/name": "devpod-gateway"},
					},
				},
			},
			"ports": []map[string]interface{}{{"protocol": "TCP", "port": backendPort}},
		},
	}
	egress := []map[string]interface{}{
		{
			"to": []map[string]interface{}{
				{
					"namespaceSelector": map[string]interface{}{
						"matchLabels": map[string]string{"kubernetes.io/metadata.name": "kube-system"},
					},
				},
			},
			"ports": []map[string]interface{}{
				{"protocol": "UDP", "port": 53},
				{"protocol": "TCP", "port": 53},
			},
		},
	}
	if session.MPIEnabled {
		sameSessionPeer := map[string]interface{}{
			"podSelector": map[string]interface{}{
				"matchLabels": selector,
			},
		}
		ingress = append(ingress, map[string]interface{}{"from": []map[string]interface{}{sameSessionPeer}})
		egress = append(egress, map[string]interface{}{"to": []map[string]interface{}{sameSessionPeer}})
	}
	if plan.NetworkProfile.AllowInternet {
		egress = append(egress, map[string]interface{}{
			"to": []map[string]interface{}{
				{
					"ipBlock": map[string]interface{}{
						"cidr":   "0.0.0.0/0",
						"except": []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
					},
				},
			},
		})
	}
	return map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      allowNetworkPolicyName(session),
			"namespace": m.cfg.Namespace,
			"labels": map[string]string{
				"app.kubernetes.io/managed-by": "csoj",
				"csoj.zjusct.io/session-id":    labelValue(session.ID),
			},
		},
		"spec": map[string]interface{}{
			"podSelector": map[string]interface{}{"matchLabels": selector},
			"policyTypes": []string{"Ingress", "Egress"},
			"ingress":     ingress,
			"egress":      egress,
		},
	}
}

func (m *Manager) nodeSelectorFor(profile config.DevPodNetworkProfile, hostNetwork bool) map[string]string {
	if hostNetwork {
		return mergeStringMaps(m.cfg.NodeSelector, m.cfg.HostNetworkNodeSelector, profile.NodeSelector)
	}
	return mergeStringMaps(m.cfg.NodeSelector, profile.NodeSelector)
}

func devPodLabels(session *models.DevPodSession) map[string]string {
	return map[string]string{
		"app":                          "csoj-devpod",
		"app.kubernetes.io/name":       "csoj-devpod",
		"app.kubernetes.io/managed-by": "csoj",
		"csoj.zjusct.io/user-id":       labelValue(session.UserID),
		"csoj.zjusct.io/username":      labelValue(session.Username),
		"csoj.zjusct.io/session-id":    labelValue(session.ID),
		"csoj.zjusct.io/k8s-resource":  labelValue(session.K8sResourceName),
		"csoj.zjusct.io/network-mode":  labelValue(session.NetworkMode),
	}
}

func allowNetworkPolicyName(session *models.DevPodSession) string {
	return "csoj-devpod-allow-" + session.K8sResourceName
}

func labelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = devpodUserPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		value = "unknown"
	}
	if len(value) > 63 {
		value = strings.Trim(value[:63], "-")
	}
	return value
}

func tolerationMaps(in []config.KubernetesToleration) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(in))
	for _, item := range in {
		toleration := map[string]interface{}{}
		if item.Key != "" {
			toleration["key"] = item.Key
		}
		if item.Operator != "" {
			toleration["operator"] = item.Operator
		}
		if item.Value != "" {
			toleration["value"] = item.Value
		}
		if item.Effect != "" {
			toleration["effect"] = item.Effect
		}
		if item.TolerationSeconds != nil {
			toleration["tolerationSeconds"] = *item.TolerationSeconds
		}
		out = append(out, toleration)
	}
	return out
}
