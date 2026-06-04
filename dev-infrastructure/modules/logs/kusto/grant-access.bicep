param ingestAccessPrincipalIds array = []

param readAccessPrincipalIds array = []

param databaseName string

@description('Name of the Kusto cluster to grant ingest to')
param kustoName string

resource database 'Microsoft.Kusto/clusters/databases@2024-04-13' existing = {
  name: '${kustoName}/${databaseName}'
}

// NOTE: The assignment names below were changed from 'grant-${guid(...)}'
// to 'grant-ingest-${guid(...)}' / 'grant-viewer-${guid(...)}' to
// distinguish ingest and viewer roles. Old assignments with the 'grant-'
// prefix are now orphaned in Azure RBAC and should be cleaned up manually
// per-cluster (they grant identical permissions to the new assignments).
resource grantIngest 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = [
  for id in ingestAccessPrincipalIds: {
    parent: database
    name: 'grant-ingest-${guid(id, databaseName)}'
    properties: {
      principalId: id
      principalType: 'App'
      role: 'Ingestor'
      tenantId: tenant().tenantId
    }
  }
]

resource grantRead 'Microsoft.Kusto/clusters/databases/principalAssignments@2024-04-13' = [
  for id in readAccessPrincipalIds: {
    parent: database
    name: 'grant-viewer-${guid(id, databaseName)}'
    properties: {
      principalId: id
      principalType: 'App'
      role: 'Viewer'
      tenantId: tenant().tenantId
    }
  }
]
