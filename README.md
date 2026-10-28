# Just-in-time access for people and agents, over Tailscale

Your agent needs the production database. It shouldn't have a standing
password for it, and neither should you.

With this, Claude Code asks for access the way a colleague would:
*"read-only on orders, 30 minutes, to check last night's refunds."* It gives
you a link, and someone approves it in the browser. Claude then connects.
Tailscale PAM injects the credentials along the way, so the agent never
sees a secret. When the time is up, the access is gone, and every request
and approval is on the record.

It works with whatever Tailscale PAM can put in front of: SSH, databases
(MySQL, PostgreSQL, MongoDB, MS SQL, CockroachDB, Elasticsearch),
Kubernetes, RDP, VNC, HTTP, TCP and AWS S3.

> **Experimental.** This is a working proof of concept, tested on a real
> tailnet. Expect rough edges.
>
> **Not an official Tailscale project.** This is an independent project and
> is not affiliated with, endorsed by, or supported by Tailscale Inc. It
> uses Tailscale's public APIs and features.

## How it fits together

There are two parts:

| | What it is | Who sets it up |
|---|---|---|
| **[jitd](jitd/README.md)** | The approval desk: approval flows, a web page to ask and approve, an API, and an MCP endpoint at `http://jit/mcp`. It runs as its own machine on your tailnet, at `http://jit`. | A tailnet admin, once |
| **Claude Code** | Connects to jitd's MCP endpoint, so Claude can see what it could ask for, ask, and wait for the answer. Nothing to install. | Everyone who uses Claude Code, with one command |

People can use jitd from the browser without Claude Code.

Under the hood, an approval sets a custom posture attribute (for example
`custom:jit_db_ro`) with an expiry on the requester's device. Your tailnet
policy decides what that attribute unlocks, including PAM permissions such
as read-only on a database. When the attribute expires, Tailscale removes
it and the access ends.

## Get started

### 1. Run jitd (an admin, once per tailnet)

Follow **[jitd/README.md](jitd/README.md)**. It takes you through it step by
step:

1. Add a `group:jit-admins` group and let people reach `tag:jit` in the policy.
2. Create an auth key and an OAuth client.
3. Build and start jitd.
4. Check that it works at http://jit/admin/setup.
5. Create an approval flow.
6. Paste the policy snippet jitd gives you.

### 2. Connect Claude Code (everyone using Claude Code)

You need:

- Tailscale running and logged in on the machine where Claude Code runs.
  (Claude Code on the web can't use it: access goes to the device that
  asks, so it has to be a device on the tailnet.)
- jitd running on your tailnet, normally at `http://jit`.

From a shell:

```sh
claude mcp add --transport http --scope user jitd http://jit/mcp
```

Then run `/mcp` in Claude Code. You should see `jitd` connected, with five
tools: `list_requestable`, `request_access`, `get_access_request`,
`wait_for_decision` and `cancel_access_request`. jitd knows who you are from
the Tailscale connection, so there is no token or login.

Try asking: *"What's in the orders database? I don't think I have access
yet."*

A few tips:

- **jitd lives somewhere other than `http://jit`?** That happens when
  another machine was already called `jit` (jitd then joins as `jit-1`) or
  when jitd runs with `-hostname`. jitd's Help page shows the name. Use it
  in the URL, for example `http://jit-1/mcp`.
- **Installed the old `tailscale-pam` plugin?** It's gone; remove it so the
  tools don't show up twice: `/plugin uninstall tailscale-pam@tailscale-jit`.
- **What can this machine reach?** Claude runs `tailscale service list`
  (add `--json`) to check, before and after an approval. You can too.

## Good to know

- **Reaching a service isn't permission to do everything on it.** The PAM
  permissions in the policy grant still apply: a read-only grant refuses
  writes.
- **Access is per device.** The attribute goes on the device that asked, so
  everyone and everything on that device has the access while it lasts.
- **Agents ask; people approve.** The MCP endpoint has no approve tool.
  However, the agent runs as you. On a flow that lets people approve their
  own requests, only its instructions stop it from approving with `curl`.
  Keep self-approval off for anything that matters.

## Working on it

jitd is one Go module. You need Go 1.26.6 or newer:

```sh
go test ./...
go build -o bin/jitd ./jitd
go run ./jitd/preview       # the jitd UI with made-up people, no tailnet needed
```

The MCP endpoint is part of jitd (`jitd/internal/jithttp/mcp.go`), so
rebuilding jitd is all it takes.

## License

[Apache License 2.0](LICENSE).

Tailscale is a trademark of Tailscale Inc. This project is not affiliated
with or endorsed by Tailscale Inc.
