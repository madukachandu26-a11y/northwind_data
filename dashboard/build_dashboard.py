import json

with open('dashboard_data_inline.json') as f:
    data_str = f.read()

with open('dashboard/dashboard_template.html') as f:
    template = f.read()

output = template.replace('__DASHBOARD_DATA__', data_str)

with open('sales_churn_dashboard.html', 'w') as f:
    f.write(output)

print("Dashboard built:", len(output), "bytes")
