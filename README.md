CADS Bridge

## Table of Contents

- [Overview](#overview)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#docker-compose)
  - [Local Development Setup](#local-development-setup)
  - [Running the Application](#running-the-application)
- [Testing](#testing)

## Overview

The CADS Bridge service is a data ingestion service for the CADS Central Data Platform.

## Technology Stack:
- .NET 10
- ASP.NET Core
- AWS S3
- AWS SQS
- AWS (LocalStack for local development)
- Docker & Docker Compose

## Prerequisites

- **.NET 10 SDK** - [Download](https://dotnet.microsoft.com/download/dotnet/10.0)
- **Docker & Docker Compose** - [Download](https://www.docker.com/products/docker-desktop)
- **Git** - [Download](https://git-scm.com/)
- **CADS Tools** - [CADS Tools](https://github.com/DEFRA/cads-tools)
- **CADS Data Seed** - [CADS DATA SEED](https://github.com/DEFRA/cads-data-seed)
- **CADS Data Service** - [CADS MIS](https://github.com/DEFRA/cads-data-service)
- **CADS Admin Frontend** - [CADS MIS](https://github.com/DEFRA/cads-admin-frontend)
- **CADS MIS (optional)** - [CADS MIS](https://github.com/DEFRA/cads-mis)
- **AWS CLI** - [Download](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

## Authentication

### API Key
Used for internal service‑to‑service calls.

### AWS STS
Used for internal service‑to‑service calls.

### Azure AD
Used for internal staff / enterprise users.

### Policies
Different scheme support applies for different groups of endpoints.

## Getting Started

### Repository Layout

Your local workspace should contain all three repos side‑by‑side:

```
D:\git\cads-data-service      # Backend (this repo)
D:\git\cads-bridge            # Cads Bridge Backend
D:\git\cads-mis               # Cads MIP Frontend
D:\git\cads-admin-frontend    # Cads Admin Frontend
D:\git\cads-tools             # Shared infra, OIDC mock, harness scripts
D:\git\cads-data-seed         # Private repository containing pseudo-anonymised reference data for testing
```

Clone them like this:

```
git clone https://github.com/DEFRA/cads-data-service.git
git clone https://github.com/DEFRA/cads-bridge.git
git clone https://github.com/DEFRA/cads-mis.git
git clone https://github.com/DEFRA/cads-admin-frontend.git
git clone https://github.com/DEFRA/cads-tools.git
git clone https://github.com/DEFRA/cads-data-seed.git

### Backend setup

**Restore NuGet packages:**

```bash
dotnet restore
```

**Create a .env file in the backend root:**

These values are used by the backend’s Docker Compose:
   
```bash
ENV=local
ApiClients__CdsApi__BasicApiKey=****
DataLoad__Salt=****
ApiClients__CdsApi__UseFakeClient=false|true
```

### Running the Application

The backend now uses a unified orchestration script found in the `cads-data-service`:

```
platform/platform.sh
```

This script starts:

- Shared infra (Postgres, Redis, LocalStack, OIDC mock)
- Imports reference data from `cads-data-seed` into the s3 bucket in LocalStack
- Backend projects (`cads-data-service`, `cads-bridge`)
- Frontend projects (`cads-mis`, `cads-admin-frontend`)
- Or any combination you want

It delegates infra to `cads-tools/harness/run-harness.sh`.

#### platform.sh — Commands

**Start shared infra only**

```
./platform/platform.sh tools
```

Starts:
- Postgres
- Redis
- LocalStack (S3, SQS)
- Imports reference data from `cads-data-seed` into the s3 bucket in LocalStack
- OIDC mock

**Start 'cads-data-service' + shared infra**

```
./platform/platform.sh cds
```

Starts:
- cads-data-service
- pgAdmin
- Liquibase migration
- Reference postgres database

**Start 'cads-bridge' + shared infra**

```
./platform/platform.sh bridge
```

Starts:
- cads-bridge

**Start 'cads-mis' + shared infra**

```
./platform/platform.sh mis
```

**Start 'cads-admin-frontend' + shared infra**

```
./platform/platform.sh admin
```

**Start everything (all frontends / backends + infra)**

```
./platform/platform.sh all
```

**Stop everything**

To stop everything without removing the postgresql data volume, use:
```
./platform/platform.sh down
```
To remove the postgresql data volume and start with a clean slate, use the `--clean` flag.
```
./platform/platform.sh down --clean
```

This stops:
- cads-data-service
- cads-bridge
- cads-mis
- cads-admin-frontend
- Shared infra

#### Mac Users — Architecture Override

Mac developers must specify their architecture when starting the backend or full platform.

**Mac Intel**

```
./platform/platform.sh backend --mac-intel
```

**Mac ARM (M1/M2/M3)**

```
./platform/platform.sh backend --mac-arm
```

**Full platform (all frontends / backends + infra)**

```
./platform/platform.sh all --mac-arm
```

Windows/Linux users do not need an override.

#### Data seeding optional argument

You can command the platform script to copy the seed data from your locally cloned cads-data-seed repository into the LocalStack S3 bucket, `cads-internal-bucket` on startup with the `--sync-data-seed` flag:

```
./platform/platform.sh backend --sync-data-seed
```

This is an optional flag and if not used the syncing of the data seed scripts into the localstack bucket will be skipped.

## Accessing Services

**'cads-data-service'**
http://localhost:5555

**'cads-bridge'**
http://localhost:5550

**'cads-mis'**
http://localhost:3000

**'cads-admin-frontend'**
http://localhost:3010

**pgAdmin**
http://localhost:16543

Login for pgAdmin:
- Email: pgadmin@pgadmin.com
- Password: (value from .env)

## Verifying Everything Is Running

```
docker compose ps
```

Or use Docker Desktop.

### Testing

A guide for the testing standards can be [found here](https://eaflood.atlassian.net/wiki/spaces/LDD/pages/6435308220/Backend+development+testing+baseline).

Because the integration tests projects must be run in sequence they must be run using a special command - an unfiltere `dotnet test` of the whole solution will fail the integration tests. 

To execute all tests except integration tests:

```
dotnet test CadsBridge.sln --filter Dependence!=testcontainers
```

To execute all integration tests:

```
dotnet test cads.integrationtests.proj --filter Dependence=testcontainers -p:BuildInParallel=false
```

Note: All test classes in *Tests.Integration.csproj projects must be decorated with Dependence=testcontainers, or this readme and check-pull-request.yml workflow must be updated

#### Test Code Coverage Reports

To generate test coverage reports locally during development, you can use the below:

Step 1: Install the report generator globally

```
dotnet tool install --global dotnet-reportgenerator-globaltool
```

Step 2: Run all tests except integration tests

```
dotnet test Cads.Cds.sln --collect:"XPlat Code Coverage" --filter Dependence!=testcontainers
```

Step 3: Create the report but exclude the `CadsBridge.Testing.Support` project

```
reportgenerator -reports:"**/TestResults/**/coverage.cobertura.xml" -targetdir:"coverage-report" -reporttypes:"Cobertura;Html;MarkdownSummary" -filefilters:"-*.g.cs" -assemblyfilters:"-*.Tests.*;-Cads.Cds.*.Testing.Support"
```

Before running steps 2 & 3, it is a good idea to delete any existing coverage report files to avoid interference from previous results.

```
Get-ChildItem -Directory -Recurse -Filter "TestResults" | Remove-Item -Recurse -Force
Get-ChildItem -Directory -Recurse -Filter "coverage-report" | Remove-Item -Recurse -Force
```

### About the licence

The Open Government Licence (OGL) was developed by the Controller of Her Majesty's Stationery Office (HMSO) to enable
information providers in the public sector to license the use and re-use of their information under a common open licence.

It is designed to encourage use and re-use of information freely and flexibly, with only a few conditions.