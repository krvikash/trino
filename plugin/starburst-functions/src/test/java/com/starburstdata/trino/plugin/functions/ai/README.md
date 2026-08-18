## Azure OpenAI test setup

- Created a new Azure OpenAI domain for testing in `starburstdata-saas-dev`(`0c3e9c6b-0cea-4cbe-a845-1bf600f3a2da`) subscription at https://portal.azure.com/#@starburstdata.com/resource/subscriptions/0c3e9c6b-0cea-4cbe-a845-1bf600f3a2da/resourceGroups/galaxy-integration-rg/providers/Microsoft.CognitiveServices/accounts/ci-sep-galaxy/overview
- Deployed following models https://ai.azure.com/resource/deployments?wsid=/subscriptions/0c3e9c6b-0cea-4cbe-a845-1bf600f3a2da/resourceGroups/galaxy-integration-rg/providers/Microsoft.CognitiveServices/accounts/ci-sep-galaxy&tid=9ac50357-7ce0-4d4f-83d3-d8a10c328c05
   - o4-mini
   - text-embedding-3-small
Connection information including an API key can be accessed by clicking on the respective deployment.

### Azure Entra ID OAuth2 (client credentials) test setup

For testing OAuth2/Entra ID auth against the Responses API shape:

- Created Entra ID app registration `sep-trino-azure-responses-oauth-test` (single-tenant, `starburstdata.onmicrosoft.com`), client-credentials flow only, no redirect URIs.
- Assigned it the `Cognitive Services OpenAI User` RBAC role, scoped to the `ci-sep-galaxy` resource only.
- Endpoint: `https://ci-sep-galaxy.openai.azure.com/openai/responses` (deployment name `o4-mini-deployment`)
- Token URL: `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token`
- Scope: `https://cognitiveservices.azure.com/.default`
- Client ID and secret are stored as GitHub Actions secrets/variables on this repo.

## Gemini test setup

Created https://console.cloud.google.com/apis/credentials/key/e2921eeb-f930-459a-9abd-f5303853d945?inv=1&invt=Ab3Czw&project=starburstdata-saas-development Gemini API key for tests

## AWS Bedrock test setup

- Created dedicated Bedrock credentials in account `starburstdata-eng-cicd-prod` (`843985043183`) for CI.
