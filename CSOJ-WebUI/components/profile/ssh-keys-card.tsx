"use client";

import useSWR from "swr";
import { KeyRound, Plus, Trash2 } from "lucide-react";

import api from "@/lib/api";
import { UserSSHKey } from "@/lib/types";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import { useState } from "react";

const fetcher = (url: string) => api.get(url).then((res) => res.data.data);

export function SSHKeysCard() {
  const { toast } = useToast();
  const { data: keys, mutate, isLoading } = useSWR<UserSSHKey[]>(
    "/user/ssh_keys",
    fetcher
  );
  const [name, setName] = useState("");
  const [publicKey, setPublicKey] = useState("");
  const [isSaving, setIsSaving] = useState(false);

  const addKey = async () => {
    if (!publicKey.trim()) return;
    setIsSaving(true);
    try {
      await api.post("/user/ssh_keys", {
        name: name.trim(),
        publicKey: publicKey.trim(),
      });
      setName("");
      setPublicKey("");
      toast({ title: "SSH key added" });
      mutate();
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Failed to add SSH key",
        description:
          error.response?.data?.message || "The public key could not be saved.",
      });
    } finally {
      setIsSaving(false);
    }
  };

  const deleteKey = async (id: string) => {
    if (!confirm("Delete this SSH key?")) return;
    try {
      await api.delete(`/user/ssh_keys/${id}`);
      toast({ title: "SSH key deleted" });
      mutate();
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Failed to delete SSH key",
        description:
          error.response?.data?.message || "The key could not be deleted.",
      });
    }
  };

  return (
    <Card className="lg:col-span-3">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <KeyRound className="h-5 w-5" />
          SSH keys
        </CardTitle>
        <CardDescription>
          Public keys are required for DevPod SSH gateway access.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="grid gap-4 lg:grid-cols-[240px_1fr_auto]">
          <div className="space-y-2">
            <Label htmlFor="ssh-key-name">Name</Label>
            <Input
              id="ssh-key-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="laptop"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="ssh-key-public">Public key</Label>
            <Textarea
              id="ssh-key-public"
              value={publicKey}
              onChange={(event) => setPublicKey(event.target.value)}
              placeholder="ssh-ed25519 AAAAC3..."
              className="min-h-20 font-mono"
            />
          </div>
          <div className="flex items-end">
            <Button onClick={addKey} disabled={isSaving || !publicKey.trim()}>
              <Plus className="h-4 w-4" />
              Add
            </Button>
          </div>
        </div>

        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Fingerprint</TableHead>
              <TableHead>Public key</TableHead>
              <TableHead className="w-12"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={4}>Loading...</TableCell>
              </TableRow>
            ) : keys && keys.length > 0 ? (
              keys.map((key) => (
                <TableRow key={key.id}>
                  <TableCell className="font-medium">{key.name}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {key.fingerprint}
                  </TableCell>
                  <TableCell className="max-w-[360px] truncate font-mono text-xs text-muted-foreground">
                    {key.public_key}
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => deleteKey(key.id)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={4} className="text-muted-foreground">
                  No SSH keys yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
