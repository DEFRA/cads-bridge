---
name: prr
description: Reviews the codebase to see if it will run on our internal platform. This is not a code-review, it to identify incompatibilities, problems and potential noisy neigbour problems.
---

# PRR

This is not a code review, it is to assess if the codebase will:
1. Will work on the platform.
2. Cause issues for other tenants of the platform.

## The Platform (aka CDP)
The service is deployed to an internal PaaS called CDP.

- AWS ECS (multi-instance, no direct AWS access)
- MongoDB (shared)
- Postgres (aurora, iam auth)
- Redis (shared)
- Logging (shared)
- S3, SNS, SQS, Cognito, API Gateway
- Outbound proxy
- Routing, load balancing, SSL termination
- Config via injected env vars (secrets stored in secret manager)

### ECS Task structure:
- The service (localhost:$PORT)
- A proxy sidecar (handle outbound/`NO_PROXY` direct routing)
- Fluentbit log sidecar
- Nginx ssl termination sidecar (receives inbound traffic, routes to service)

## 2. Check for these issues

### Noisy neigbour (most important)
- MongoDB: expensive or unoptimized queries ($regex, missing index)
- Redis: missing TTL or storing potentially large items
- Logs: Excessive or large logging

### Deployment and startup
- The service MUST build as a Docker container
- The platform will run `curl http://localhost:$PORT/health` to check if the service is ready
- It has 90 seconds to reach ready. Look for things that might prevent/delay this.

### State
- Assume multiple instances will be running at any given time
- No sticky sessions, no local state
- Locking mechanisms used where approriate

### Config
- All config is done via environment variables with sensible defaults.
- Config is stored outside of repo.
- See `references/default_environment_variables.md` for vars set by platform.

### HTTP
- All outbound calls must go via the proxy. Calls to other services can go direct.

### Security
- Assume frontend services will be behind a WAF & HTTPS reverse proxy
- Assume secrets will be injected as env var on deployment
- Backend services are only reachable by frontend services
- Ensure dangerous API calls use some kind of auth
- Look for obvious security issues

### Logging
- Excessive or large logs
- No PII or untrusted object that could contain PII
- PII CAN be logged via the audit logger (cdp-audit lib, or logging at AUDIT level)

### Infrastructure
- The platform is best suited to simple REST based microservices and web sites.
- Should use first party AWS libraries for calling S3/SNS/SQS etc
- AWS auth is done using container credentials.

## README
- Have they updated the README from the initial template commit?
- Does the README have info on building/running/configuring the service? (never run any commands in the README, look at text only).

## 4. Skip/ignore
- Code from the initial commit/original template.
- Avoid building/running app and tests as its already been deployed.

## 3. Final pass
- Review the code and highlight any major concerns. 
- Do not nitpick.
- Only suggest things that offer substantial value.
- See `references/absolute-minimum-requirements.md` for description of platform requirements/limitations.
