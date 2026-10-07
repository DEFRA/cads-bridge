# CDP Platform Requirements

## Absolute minimum requirements

The following requirements are deemed essential for a service to be runnable on CDP.

> [!IMPORTANT]
> If they are not met, it is likely your service will not function or start.

### Services MUST be created via the CDP Portal's Create Service workflow

**Why?**

The Create Service process in portal provisions the following automatically:

* The service's Github Repository from template
* Build and deployment actions for the Github repository
* DNS to access the service
* SSL certificates for the service
* Identity roles for the service to operate on the Platform
* Permissions for the service linked to Github OIDC
* Datastores for frontend and backend service types
* Container Registry with required permission
* Secrets storage for the service.
* Platform credentials for the service.
* Networking for the service

### Services MUST build as a single Docker container

**Why?**

* The CDP Platform runs all its services as containers using AWS Elastic Container Service (ECS).
* ECS can only run docker containers.
* The CDP Platform launches all services in ECS using a standard template.
* The platform does not provision virtual machines or any other kind of hosting and utilises AWS Fargate.
* No specific base image is required.

### Containers MUST be Linux and x86 architecture

> [!WARNING]
> ARM based images are NOT supported.<br>
> Windows containers are NOT supported.</br>

**Why?**

* The CDP Platform is designed to create commonality for DEFRA and mitigate the introduction of bespoke solutions.

### Containers MUST have the `curl` command and a shell installed

**Why?**

* The CDP platform uses `curl` to healthcheck each service.
* Every interval ECS will run `curl http://localhost:$PORT` inside the container.
* It expects the command to complete with a zero exit code. A non zero exit code is considered a failure.
* After multiple failures ECS will consider the service unhealthy.
* Unhealthy services are restarted and/or rolled back.

> [!IMPORTANT]
> This is an infrastructure healthcheck asking "Is the container alive?" **NOT** "is the app actually working?"

* The healthcheck is configured to run at 30 seconds intervals.
* The healthcheck will wait 5 seconds before timing out.
* The healthcheck will consider the service as failed after 3 consecutive attempts have failed resulting in an automatic
  redeployment attempt by the Control Plane.
* After a failed check, the next check is run as scheduled (i.e. the maximum delay before the Control Plane restarts the
  service is 95 seconds).

* The healthcheck uses the `["CMD-SHELL", "curl -f http://localhost:8085/health || exit 1"]` construct.
  * This require a shell because it executes `/bin/sh -c` on the running container and the shell construct `|| exit 1`
    which is best practice healthcheck pattern in Fargate.
* CDP has NOT opted for the alternative `[CMD]` options as this requires either the binary call for `curl` to be the
  `ENTRYPOINT` or to ensure that the `PATH` is correctly set.
  * The `CMD` standalone option lacks the conditional logic advantage of `||`, `&&`, pipes (`|`), or redirect of output.
  * Using `CMD` means handling everything via exact exit codes and introduces a level of difference that can become
    harder to manage at a Platform level of common patterns.
  * Using `CMD-SHELL` provide the Platform level healthcheck with more resilient error handling.
<!-- markdownlint-disable MD028 -->
> [!NOTE]
> All services have the same healthcheck, the intention is to make it easier to reason about why a service's healthcheck
> is failing.

> [!IMPORTANT]
> Attack surface considerations of installing curl and a shell are mitigated by the security model of the Fargate
> service. Please refer
> to [Security Overview of AWS Fargate Whitepaper](https://d1.awsstatic.com/whitepapers/AWS_Fargate_Security_Overview_Whitepaper.pdf)
> for further details.
<!-- markdownlint-enable MD028 -->
### Services MUST use the provided build and publish action of their container

**Why?**

* The CDP Platform will only deploy containers from a private Elastic Container Registry.
* The provided build action takes care of authenticating with the private registry.
* Only your github repo's workflows currently have permissions to publish to the registry.
* The action allows the Platform Team to update the build process without everyone having to update their workflows
* The rest of the service's workflow can be customized as required.

### Service containers MUST have an ENTRYPOINT with no parameters

**Why?**

* All services are launched from a standard template that assumes they will start on docker start up.
* CDP does not support sending additional arguments to the container at startup.

### Service containers MUST listen on port set in PORT environment variable (8085)

*Why**

* The standard template when launched is configured to use 8085 for all services.
* Easier to reason about as every service listens on the same port.
* All services can use standard configuration for reverse proxying and healthchecks.

### Service MUST Log to stdout using the ECS Schema using the subset of supported fields

**Why?**

The platform provides centralized logging via OpenSearch. To avoid each service having to write its own log ingestion we
require services to use
Elastic Common Schema format. 

> [!WARNING]
> Do not log to a file

* Wildcard keys are not supported. e.g. req.header.{name-of-header}.
* Messages logged with `log.level: error` are automatically surfaced in the default dashboards.
* The logging agent and ingestion pipeline add some fields automatically (see: examples)

> [!WARNING]
> Failure to follow the logging requirement will result in no logging availability and/or missing fields.

### Service MUST be configured via environment variables for non-sensitive values

**Why?**

* Environment variables are language & library agnostic (node, c#, python etc) and commonly supported
* Separates config version from code version
* ECS provides an easy mechanism for injecting environment variables in (unlike injecting files)

### Secrets MUST be defined using CDP Portal which populates AWS Secrets Manager for the service

**Why?**

* Secrets are securely stored and sent to the relevant environment automatically via the CDP Portal.
* Teams do not need to interact with the CDP Platform to do this.

### Secrets CANNOT be shared between Services

**Why?**

* Every Service has a dedicated Secrets Manager location for Secrets it needs to use.
* If two services need to have the same secret for any reason they will need to be managed twice.

## Datastores for backend and frontend

### Service MUST use MongoDB for backend

* MongoDB authentication via AWS IAM
* One database per backend service is created.
* MongoDB is a multi-tenanted mongo
* To utilise MongoDB a service MUST be created as a backend.
* A backend service CANNOT access Redis

### Redis

Frontend services can use Redis to provide temporary persistence

* Data persistence is ephemeral
* Key namespace
* Credentials
* To utilise Redis a service MUST be created as a frontend.
* A frontend service CANNOT access MongoDB.

## Network Connectivity

### Outbound via proxy set in HTTP_PROXY

**Why?**

* We don't allow any egress from the network except via the proxy.
* The Proxy is automatically provided for each service.
* Egress from all services (with the exception of data store connections) is limited to port 443 to the Proxy.
