$token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIzIiwiZXhwIjoxNzc5Nzk3MDE5fQ.aDaQsicZUd4WYC_Zgm9HLx1q7uZqWcnC3CYTHf10IGY'
$h=@{Authorization="Bearer $token"; "Content-Type"="application/json"}
try {
    $model = Invoke-RestMethod -Uri 'http://localhost:8000/api/models/100' -Method Get -Headers $h
    Write-Host "MODEL_STATUS=$($model.status)"
    $deployPayload='{"target_connection_id":58,"target_schema":"rpt"}'
    $deployed=Invoke-RestMethod -Uri 'http://localhost:8000/api/models/100/deploy' -Method Post -Headers $h -Body $deployPayload
    Write-Host "DEPLOY_OK"
} catch {
    Write-Host "ERROR: $_"
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        Write-Host "RESPONSE: $($reader.ReadToEnd())"
    }
}
