package devpod

import (
	"testing"

	"github.com/ZJUSCT/CSOJ/internal/config"
	"github.com/ZJUSCT/CSOJ/internal/database/models"
)

func testManager() *Manager {
	return NewManager(config.DevPod{
		Enabled: true,
		Gateway: config.DevPodGateway{
			Host: "gateway.example.com",
		},
		HostNetworkNodeSelector: map[string]string{
			"csoj.zjusct.io/network-profile": "mpi-hostnet",
		},
		Images: []config.DevPodImage{
			{Name: "Ubuntu", Image: "ubuntu:24.04", Allowed: true},
			{Name: "MPI", Image: "registry.local/csoj/mpi:latest", Allowed: true, Profiles: []string{"mpi"}},
		},
		NetworkProfiles: []config.DevPodNetworkProfile{
			{Name: "default"},
			{Name: "internet", AllowInternet: true},
			{Name: "mpi-hostnet", HostNetwork: true, AdminOnly: true, MPI: true},
		},
	})
}

func testSSHKeys() []models.UserSSHKey {
	return []models.UserSSHKey{{
		ID:          "key-1",
		UserID:      "user-1",
		PublicKey:   "ssh-ed25519 YWJj test",
		Fingerprint: "SHA256:test",
	}}
}

func TestValidateCreateRejectsHostNetworkForNormalUser(t *testing.T) {
	manager := testManager()
	user := models.User{ID: "user-1", Username: "alice"}

	_, err := manager.ValidateCreateRequest(user, testSSHKeys(), 0, CreateRequest{
		Image:          "registry.local/csoj/mpi:latest",
		NetworkProfile: "mpi-hostnet",
		HostNetwork:    true,
		MPIEnabled:     true,
	})
	if err == nil {
		t.Fatalf("expected hostNetwork request to be rejected")
	}
	_, code, _ := ErrorResponse(err)
	if code != "NETWORK_PROFILE_FORBIDDEN" && code != "HOST_NETWORK_FORBIDDEN" {
		t.Fatalf("unexpected error code: %s", code)
	}
}

func TestValidateCreateAllowsPrivilegedMPIHostNetwork(t *testing.T) {
	manager := testManager()
	user := models.User{ID: "user-1", Username: "alice", Tags: "devpod-admin"}

	plan, err := manager.ValidateCreateRequest(user, testSSHKeys(), 0, CreateRequest{
		Image:          "registry.local/csoj/mpi:latest",
		NetworkProfile: "mpi-hostnet",
		HostNetwork:    true,
		MPIEnabled:     true,
	})
	if err != nil {
		t.Fatalf("expected privileged hostNetwork request to pass: %v", err)
	}
	if !plan.Request.HostNetwork || !plan.Request.MPIEnabled {
		t.Fatalf("expected hostNetwork and MPI to be enabled: %#v", plan.Request)
	}
}

func TestBuildDevPodManifestUsesSafePodSpec(t *testing.T) {
	manager := testManager()
	user := models.User{ID: "user-1", Username: "Alice_User"}
	plan, err := manager.ValidateCreateRequest(user, testSSHKeys(), 0, CreateRequest{
		Image: "ubuntu:24.04",
		Env:   map[string]string{"OMP_NUM_THREADS": "2"},
	})
	if err != nil {
		t.Fatalf("validate create request: %v", err)
	}
	session, err := manager.BuildSession(user, plan)
	if err != nil {
		t.Fatalf("build session: %v", err)
	}
	manifest := manager.BuildDevPodManifest(&session, plan)
	spec := manifest["spec"].(map[string]interface{})
	pod := spec["pod"].(map[string]interface{})
	podSpec := pod["spec"].(map[string]interface{})
	if podSpec["hostNetwork"].(bool) {
		t.Fatalf("default pod must not enable hostNetwork")
	}
	containers := podSpec["containers"].([]map[string]interface{})
	securityContext := containers[0]["securityContext"].(map[string]interface{})
	if securityContext["allowPrivilegeEscalation"].(bool) {
		t.Fatalf("container must not allow privilege escalation")
	}
	if securityContext["runAsNonRoot"].(bool) != true {
		t.Fatalf("container must run as non-root")
	}
	if _, ok := podSpec["volumes"]; ok {
		t.Fatalf("CSOJ must not let users inject volumes or hostPath through DevPod create")
	}
}

func TestBuildAllowNetworkPolicyInternetProfile(t *testing.T) {
	manager := testManager()
	user := models.User{ID: "user-1", Username: "alice"}
	plan, err := manager.ValidateCreateRequest(user, testSSHKeys(), 0, CreateRequest{
		Image:          "ubuntu:24.04",
		NetworkProfile: "internet",
	})
	if err != nil {
		t.Fatalf("validate create request: %v", err)
	}
	session, err := manager.BuildSession(user, plan)
	if err != nil {
		t.Fatalf("build session: %v", err)
	}
	policy := manager.BuildAllowNetworkPolicy(&session, plan)
	spec := policy["spec"].(map[string]interface{})
	egress := spec["egress"].([]map[string]interface{})
	foundInternet := false
	for _, rule := range egress {
		for _, peer := range rule["to"].([]map[string]interface{}) {
			if _, ok := peer["ipBlock"]; ok {
				foundInternet = true
			}
		}
	}
	if !foundInternet {
		t.Fatalf("internet profile should include public egress")
	}
}

func TestBuildAllowNetworkPolicyDefaultProfileNoInternet(t *testing.T) {
	manager := testManager()
	user := models.User{ID: "user-1", Username: "alice"}
	plan, err := manager.ValidateCreateRequest(user, testSSHKeys(), 0, CreateRequest{
		Image: "ubuntu:24.04",
	})
	if err != nil {
		t.Fatalf("validate create request: %v", err)
	}
	session, err := manager.BuildSession(user, plan)
	if err != nil {
		t.Fatalf("build session: %v", err)
	}
	policy := manager.BuildAllowNetworkPolicy(&session, plan)
	spec := policy["spec"].(map[string]interface{})
	egress := spec["egress"].([]map[string]interface{})
	for _, rule := range egress {
		for _, peer := range rule["to"].([]map[string]interface{}) {
			if _, ok := peer["ipBlock"]; ok {
				t.Fatalf("default profile must not include public egress")
			}
		}
	}
}
