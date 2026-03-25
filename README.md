CADS Bridge

## Table of Contents

* [Overview](#overview)
* [Technology Stack](#technology-stack)
* [Prerequisites](#prerequisites)
* [Getting Started](#docker-compose)
* [Testing](#testing)
* [SonarCloud](#sonarcloud)
* [Running](#running)
* [Dependabot](#dependabot)

## Overview

The CADS Bridge service is a data ingestion service for the CADS Central Data Platform.

## Technology Stack:
- .NET 10
- ASP.NET Core
- AWS S3
- AWS (LocalStack for local development)
- Docker & Docker Compose

## Prerequisites

- **.NET 10 SDK** - [Download](https://dotnet.microsoft.com/download/dotnet/10.0)
- **Docker & Docker Compose** - [Download](https://www.docker.com/products/docker-desktop)
- **Git** - [Download](https://git-scm.com/)

## Getting Started

A Docker Compose template is in [docker-compose.yml](docker-compose.yml).

A local environment with:

- Localstack for AWS services (S3)
- This service.

If running locally on MacOS run the following command to allow executalbe access to the localstack start up script:

```bash
chmod +x compose/start-localstack.sh 
```

To start the local environment run:

Windows
```bash
docker-compose -f docker-compose.yml -f docker-compose.override.yml up --build -d;
```
MacOS Arm/Silicon
```bash
docker-compose -f docker-compose.yml -f docker-compose.override.mac.arm.yml up --build -d;
```
MacOS Intel
```bash
docker-compose -f docker-compose.yml -f docker-compose.override.mac.intel.yml up --build -d;
```

To clean up the local environment run:

```bash
docker compose up --down -v
```

### Testing

Run the tests with:

Tests run by running a full `WebApplication` backed by [Ephemeral MongoDB](https://github.com/asimmon/ephemeral-mongo).
Tests do not use mocking of any sort and read and write from the in-memory database.

```bash
dotnet test
````

### Running

Run CDP-Deployments application:
```bash
dotnet run --project CadsBridge --launch-profile Development
```

### SonarCloud

Example SonarCloud configuration are available in the GitHub Action workflows.

### Dependabot

We have added an example dependabot configuration file to the repository. You can enable it by renaming
the [.github/example.dependabot.yml](.github/example.dependabot.yml) to `.github/dependabot.yml`


### About the licence

The Open Government Licence (OGL) was developed by the Controller of Her Majesty's Stationery Office (HMSO) to enable
information providers in the public sector to license the use and re-use of their information under a common open
licence.

It is designed to encourage use and re-use of information freely and flexibly, with only a few conditions.
