# CADS Bridge Runbook

## Service Overview

### What the Service does

### High-level Architecture

#### System Context Diagram

#### Container Diagram

### Dependencies

## Ownership & Contacts

**Primary Team:** TBD

**On-call Escalation:** TBD

**Communication Channels:** TBD

## Operational Characteristics

**Expected Throughput:** TBD

**Latency SLOs:** TBD

**Rate Limits:** TBD

**Scheduled Operations:** TBD

**Deployment:**
All deployment operations driven through the CDP portal: https://portal.cdp-int.defra.cloud/services/cads-bridge

## Monitoring & Observability

### Dashboards

**Service Dashboard** TBD

**Custom Dashboard** TBD

### Key Metrics

**HTTP Request Metrics (via ApplicationMetrics):** TBD

**Health Check Metrics:** TBD

**Queue Processing Metrics:** TBD

### Log Locations

**Local Development:** TBD

**Deployed environments:** TBD

### Health Check Endpoints

- **Primary**: `GET /health/details` - Returns comprehensive system health
- **Liveness**: `GET /health` - Simple aliveness check

### Alerts Configuration

**Standard CDP Alerts:** TBD

**Service-specific Alerts:** TBD

## Common Failure Modes & Incident Procedures

**TBD** - No specific failure modes or incidents have been identified yet. This section will be updated as operational experience is gained and patterns emerge.

## Release & Rollback Procedures

### Deployment Process
All deployments handled through the CDP Portal

### Rollback Process
Rollbacks are handled be redeploying the previous version through the CDP Portal

### Validation Steps
- Health check returns "Healthy"
- API endpoints respond correctly
- Queue processing resumed
- External integrations working
- No error spike in logs

## Configuration & Secrets

### Configuration Sources
- **Primary**: Environment variables
- **Local**: `docker-compose.override.yml`
- **Deployed**: Environment variables defined in `cdp-app-config` repo

### Secret Rotation
1. Update secrets in secret management system
2. Update environment configuration
3. Restart service to pick up changes
4. Verify connectivity to all dependent services

## Security & Compliance

### Authentication/Authorization

### API Gateway Client Credentials
 - **Recycling client secrets**: The team can request for the client secret to be recycled by the CDP team. The CDP team can rotate the credentials by creating a new one and then expiring the old one after confirmation that the new one is in use. CDP do not enforce any fixed rotation period.

### Data Classification
- **PII**: Contains no PII data
- **Business Critical**: Essential for livestock traceability
- **Retention**: Follow DEFRA data retention policies

### Audit Logging
- All API requests logged with correlation IDs
- Queue message processing tracked
- Health check executions recorded
- Configuration changes audited

### Compliance Notes
- GDPR compliance for personal data
- DEFRA data handling requirements
- AWS security best practices

## Appendices