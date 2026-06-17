"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import useSWR from "swr";
import { ArrowLeft, Loader2, Rocket } from "lucide-react";

import api from "@/lib/api";
import { DevPodOptions } from "@/lib/types";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";

const fetcher = (url: string) => api.get(url).then((res) => res.data.data);

function parseEnv(raw: string) {
  const env: Record<string, string> = {};
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const index = trimmed.indexOf("=");
    if (index <= 0) {
      throw new Error(`Invalid env line: ${trimmed}`);
    }
    env[trimmed.slice(0, index)] = trimmed.slice(index + 1);
  }
  return env;
}

function parseCommand(raw: string) {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  if (trimmed.startsWith("[")) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
      throw new Error("Command JSON must be an array of strings");
    }
    return parsed;
  }
  return trimmed.split(/\s+/);
}

export default function NewDevPodPage() {
  const router = useRouter();
  const { toast } = useToast();
  const { data: options, isLoading } = useSWR<DevPodOptions>(
    "/devpods/options",
    fetcher
  );
  const defaults = options?.defaults;
  const [displayName, setDisplayName] = useState("");
  const [image, setImage] = useState("");
  const [cpu, setCPU] = useState(1);
  const [memoryMB, setMemoryMB] = useState(2048);
  const [gpu, setGPU] = useState(0);
  const [persistent, setPersistent] = useState(false);
  const [storageGB, setStorageGB] = useState(10);
  const [idleTimeoutSeconds, setIdleTimeoutSeconds] = useState(3600);
  const [maxLifetimeSeconds, setMaxLifetimeSeconds] = useState(86400);
  const [networkProfile, setNetworkProfile] = useState("default");
  const [mpiEnabled, setMPIEnabled] = useState(false);
  const [hostNetwork, setHostNetwork] = useState(false);
  const [envText, setEnvText] = useState("");
  const [commandText, setCommandText] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (!options) return;
    setImage((current) => current || defaults?.image || options.images[0]?.image || "");
    setCPU((current) => current || defaults?.cpu || 1);
    setMemoryMB((current) => current || defaults?.memory_mb || 2048);
    setStorageGB((current) => current || defaults?.storage_gb || 10);
    setPersistent((current) => current || Boolean(defaults?.persistent));
    setIdleTimeoutSeconds(defaults?.idle_timeout_seconds || 3600);
    setMaxLifetimeSeconds(defaults?.max_lifetime_seconds || 86400);
    setNetworkProfile((current) => current || defaults?.network_profile || "default");
    setCommandText((current) =>
      current || (defaults?.command ? defaults.command.join(" ") : "sleep infinity")
    );
  }, [options, defaults]);

  const selectedProfile = options?.network_profiles.find(
    (profile) => profile.name === networkProfile
  );

  const submit = async () => {
    if (!options) return;
    setIsSubmitting(true);
    try {
      const payload = {
        displayName,
        image,
        cpu,
        memoryMB,
        gpu,
        persistent,
        storageGB,
        idleTimeoutSeconds,
        maxLifetimeSeconds,
        networkProfile,
        mpiEnabled,
        hostNetwork,
        env: parseEnv(envText),
        command: parseCommand(commandText),
      };
      const response = await api.post("/devpods", payload);
      toast({ title: "DevPod creation requested" });
      router.push(`/devpods?id=${response.data.data.id}`);
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Failed to create DevPod",
        description: error.response?.data?.message || error.message,
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  if (isLoading || !options) {
    return <Skeleton className="h-96 w-full" />;
  }

  if (!options.enabled) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>DevPods are disabled</CardTitle>
          <CardDescription>
            The backend has not enabled the DevPod module.
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <Button variant="ghost" onClick={() => router.push("/devpods")}>
        <ArrowLeft className="h-4 w-4" />
        Back
      </Button>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-2xl">
            <Rocket className="h-6 w-6" />
            New DevPod
          </CardTitle>
          <CardDescription>
            Create an SSH-accessible development container.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="display-name">Name</Label>
              <Input
                id="display-name"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                placeholder="my-dev-env"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="image">Image</Label>
              <select
                id="image"
                value={image}
                onChange={(event) => setImage(event.target.value)}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm shadow-sm"
              >
                {options.images.map((item) => (
                  <option key={item.image} value={item.image}>
                    {item.name} ({item.image})
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-4">
            <div className="space-y-2">
              <Label htmlFor="cpu">CPU</Label>
              <Input
                id="cpu"
                type="number"
                min={1}
                max={options.limits.max_cpu_per_pod}
                value={cpu}
                onChange={(event) => setCPU(Number(event.target.value))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="memory">Memory MB</Label>
              <Input
                id="memory"
                type="number"
                min={128}
                max={options.limits.max_memory_mb_per_pod}
                step={128}
                value={memoryMB}
                onChange={(event) => setMemoryMB(Number(event.target.value))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="gpu">GPU</Label>
              <Input
                id="gpu"
                type="number"
                min={0}
                max={options.limits.max_gpu_per_pod}
                value={gpu}
                onChange={(event) => setGPU(Number(event.target.value))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="storage">Storage GB</Label>
              <Input
                id="storage"
                type="number"
                min={1}
                max={options.limits.max_storage_gb_per_pod}
                disabled={!persistent}
                value={storageGB}
                onChange={(event) => setStorageGB(Number(event.target.value))}
              />
            </div>
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="network-profile">Network profile</Label>
              <select
                id="network-profile"
                value={networkProfile}
                onChange={(event) => {
                  const next = event.target.value;
                  setNetworkProfile(next);
                  const profile = options.network_profiles.find((item) => item.name === next);
                  setHostNetwork(Boolean(profile?.host_network));
                  setMPIEnabled(Boolean(profile?.mpi));
                }}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm shadow-sm"
              >
                {options.network_profiles.map((profile) => (
                  <option key={profile.name} value={profile.name}>
                    {profile.name}
                    {profile.allow_internet ? " · internet" : ""}
                    {profile.host_network ? " · hostNetwork" : ""}
                  </option>
                ))}
              </select>
            </div>
            <div className="grid grid-cols-3 gap-3 pt-7 text-sm">
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={persistent}
                  onChange={(event) => setPersistent(event.target.checked)}
                />
                Persistent
              </label>
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={mpiEnabled}
                  onChange={(event) => setMPIEnabled(event.target.checked)}
                />
                MPI
              </label>
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={hostNetwork}
                  disabled={!selectedProfile?.host_network}
                  onChange={(event) => setHostNetwork(event.target.checked)}
                />
                Host network
              </label>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="idle-timeout">Idle timeout seconds</Label>
              <Input
                id="idle-timeout"
                type="number"
                min={0}
                value={idleTimeoutSeconds}
                onChange={(event) => setIdleTimeoutSeconds(Number(event.target.value))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="max-lifetime">Max lifetime seconds</Label>
              <Input
                id="max-lifetime"
                type="number"
                min={60}
                value={maxLifetimeSeconds}
                onChange={(event) => setMaxLifetimeSeconds(Number(event.target.value))}
              />
            </div>
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="env">Environment</Label>
              <Textarea
                id="env"
                value={envText}
                onChange={(event) => setEnvText(event.target.value)}
                placeholder={"OMP_NUM_THREADS=2\nFOO=bar"}
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="command">Command</Label>
              <Textarea
                id="command"
                value={commandText}
                onChange={(event) => setCommandText(event.target.value)}
                placeholder="sleep infinity"
                className="font-mono"
              />
            </div>
          </div>

          <div className="flex justify-end">
            <Button onClick={submit} disabled={isSubmitting}>
              {isSubmitting && <Loader2 className="h-4 w-4 animate-spin" />}
              Create DevPod
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
