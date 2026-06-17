$token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIzIiwiZXhwIjoxNzc5Nzk3MDE5fQ.aDaQsicZUd4WYC_Zgm9HLx1q7uZqWcnC3CYTHf10IGY'
$h=@{Authorization="Bearer $token"}
Invoke-RestMethod -Uri 'http://localhost:8000/api/connections' -Method Get -Headers $h | Select-Object id, name, type | ConvertTo-Json
