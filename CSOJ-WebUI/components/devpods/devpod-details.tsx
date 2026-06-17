"use client";

import type { ReactNode } from "react";
import { useRouter } from "next/navigation";
import useSWR from "swr";
import { format, formatDistanceToNow } from "date-fns";
import {
  ArrowLeft,
  Cpu,
  HardDrive,
  Layers,
  MemoryStick,
  Play,
  Square,
  Terminal,
  Trash2,
} from "lucide-react";

import api from "@/lib/api";
import { DevPodSession } from "@/lib/types";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { CopyButton } from "@/components/ui/shadcn-io/copy-button";
import { DevPodStatusBadge } from "@/components/devpods/devpod-status-badge";
import { Separator } from "@/components/ui/separator";
import { useToast } from "@/hooks/use-toast";

const fetcher = (url: string) => api.get(url).then((res) => res.data.data);

export function DevPodDetails({ id }: { id: string }) {
  const router = useRouter();
  const { toast } = useToast();
  const { data: pod, isLoading, mutate } = useSWR<DevPodSession>(
    `/devpods/${id}`,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: logs } = useSWR<{ logs: string }>(
    pod && pod.status !== "Deleted" ? `/devpods/${id}/logs` : null,
    fetcher,
    { refreshInterval: 5000, shouldRetryOnError: false }
  );

  const action = async (verb: "start" | "stop" | "delete") => {
    try {
      if (verb === "delete") {
        if (!confirm("Delete this DevPod?")) return;
        await api.delete(`/devpods/${id}`);
      } else {
        await api.post(`/devpods/${id}/${verb}`);
      }
      toast({ title: `DevPod ${verb} requested` });
      if (verb === "delete") {
        router.push("/devpods");
      } else {
        mutate();
      }
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: `Failed to ${verb} DevPod`,
        description: error.response?.data?.message || "The request failed.",
      });
    }
  };

  if (isLoading || !pod) {
    return <Skeleton className="h-96 w-full" />;
  }

  return (
    <div className="space-y-6">
      <Button variant="ghost" onClick={() => router.push("/devpods")}>
        <ArrowLeft className="h-4 w-4" />
        Back
      </Button>

      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div>
                <CardTitle className="flex items-center gap-2 text-2xl">
                  <Terminal className="h-6 w-6" />
                  {pod.displayName || pod.name}
                </CardTitle>
                <CardDescription className="font-mono">{pod.name}</CardDescription>
              </div>
              <DevPodStatusBadge status={pod.status} />
            </div>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="rounded-md border bg-muted/40 p-4">
              <div className="mb-2 text-sm font-medium">SSH command</div>
              <div className="flex items-center gap-2">
                <code className="block flex-1 overflow-x-auto rounded bg-background px-3 py-2 text-sm">
                  {pod.sshCommand}
                </code>
                <CopyButton content={pod.sshCommand} />
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-3">
              <InfoItem icon={<Cpu className="h-4 w-4" />} label="CPU" value={`${pod.cpu}`} />
              <InfoItem
                icon={<MemoryStick className="h-4 w-4" />}
                label="Memory"
                value={`${pod.memoryMB} MB`}
              />
              <InfoItem
                icon={<HardDrive className="h-4 w-4" />}
                label="Storage"
                value={pod.persistent ? `${pod.storageGB} GB` : "Ephemeral"}
              />
            </div>

            <Separator />

            <div className="grid gap-4 md:grid-cols-2">
              <InfoRow label="Image" value={pod.image} />
              <InfoRow label="Namespace" value={pod.namespace} />
              <InfoRow label="Network profile" value={pod.networkProfile} />
              <InfoRow label="Kubernetes resource" value={pod.k8sResourceName} />
              <InfoRow label="MPI" value={pod.mpiEnabled ? "Enabled" : "Disabled"} />
              <InfoRow
                label="Host network"
                value={pod.hostNetwork ? "Enabled" : "Disabled"}
              />
              <InfoRow
                label="Expires"
                value={
                  pod.expiresAt
                    ? format(new Date(pod.expiresAt), "yyyy-MM-dd HH:mm:ss")
                    : "-"
                }
              />
              <InfoRow
                label="Last activity"
                value={
                  pod.lastActivityAt
                    ? formatDistanceToNow(new Date(pod.lastActivityAt), { addSuffix: true })
                    : "-"
                }
              />
            </div>
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Actions</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {pod.status === "Stopped" ? (
                <Button className="w-full" onClick={() => action("start")}>
                  <Play className="h-4 w-4" />
                  Start
                </Button>
              ) : (
                <Button
                  className="w-full"
                  variant="outline"
                  disabled={pod.status === "Deleted" || pod.status === "Deleting"}
                  onClick={() => action("stop")}
                >
                  <Square className="h-4 w-4" />
                  Stop
                </Button>
              )}
              <Button
                className="w-full"
                variant="destructive"
                disabled={pod.status === "Deleted"}
                onClick={() => action("delete")}
              >
                <Trash2 className="h-4 w-4" />
                Delete
              </Button>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Layers className="h-5 w-5" />
                Connection
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <InfoRow label="SSH user" value={pod.sshUser} />
              <InfoRow label="SSH host" value={pod.sshHost} />
              <InfoRow label="SSH port" value={`${pod.sshPort}`} />
            </CardContent>
          </Card>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Logs</CardTitle>
          <CardDescription>Recent workload container logs.</CardDescription>
        </CardHeader>
        <CardContent>
          <pre className="min-h-48 overflow-auto rounded-md bg-muted p-4 text-xs">
            {logs?.logs || "No logs available yet."}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

function InfoItem({
  icon,
  label,
  value,
}: {
  icon: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-md border p-4">
      <div className="mb-2 flex items-center gap-2 text-sm text-muted-foreground">
        {icon}
        {label}
      </div>
      <div className="font-medium">{value}</div>
    </div>
  );
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="break-all text-right font-mono">{value}</span>
    </div>
  );
}
