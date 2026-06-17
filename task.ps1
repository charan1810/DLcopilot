$token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIzIiwiZXhwIjoxNzc5Nzk3MDE5fQ.aDaQsicZUd4WYC_Zgm9HLx1q7uZqWcnC3CYTHf10IGY'
$h=@{Authorization="Bearer $token"; "Content-Type"="application/json"}
$fromClause=@"
(
  SELECT
    o.order_id,
    o.order_number,
    o.order_ts::date AS order_date,
    date_trunc('month', o.order_ts)::date AS order_month,
    o.customer_id,
    concat_ws(' ', c.first_name, c.last_name) AS customer_name,
    c.loyalty_tier AS customer_segment,
    ca.state_code AS region,
    ca.city,
    o.sales_channel,
    COALESCE(ia.item_count, 0) AS item_count,
    COALESCE(ia.total_quantity, 0) AS total_quantity,
    o.subtotal_amount AS gross_amount,
    o.discount_amount,
    o.tax_amount
  FROM src.orders o
  LEFT JOIN src.customers c ON c.customer_id = o.customer_id
  LEFT JOIN LATERAL (
    SELECT x.city, x.state_code
    FROM src.customer_addresses x
    WHERE x.customer_id = o.customer_id
    ORDER BY x.is_default DESC, x.created_at DESC
    LIMIT 1
  ) ca ON TRUE
  LEFT JOIN (
    SELECT order_id, COUNT(*) AS item_count, SUM(quantity) AS total_quantity
    FROM src.order_items
    GROUP BY order_id
  ) ia ON ia.order_id = o.order_id
) q
"@
$patch = @{ source_schema='src'; source_tables=@('orders'); transformations=@{ view_from_clause=$fromClause } } | ConvertTo-Json -Depth 6
$updated=Invoke-RestMethod -Uri 'http://localhost:8000/api/models/100' -Method Put -Headers $h -Body $patch
Write-Host "UPDATED_VERSION=$($updated.version)"
Write-Host "UPDATED_SQL:" $updated.generated_sql
$deployPayload='{"target_connection_id":58,"target_schema":"rpt"}'
try {
  $deployed=Invoke-RestMethod -Uri 'http://localhost:8000/api/models/100/deploy' -Method Post -Headers $h -Body $deployPayload
  Write-Host "DEPLOY_OK STATUS=$($deployed.status) TARGET=$($deployed.target_schema)"
} catch {
  Write-Host "DEPLOY_ERR:" $_.Exception.Message
  if($_.Exception.Response){ $r=[System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream()); Write-Host $r.ReadToEnd() }
}
