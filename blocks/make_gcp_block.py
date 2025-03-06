from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# copy your own service_account_info dictionary from the json file you downloaded from google
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    service_account_info={
        "type": "service_account",
        "project_id": "paris-air-pollution-quangnc",
        "private_key_id": "5899acc5f4b616cf36da6208f5f89bd318dada4f",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDRddJRib54JsmO\nzi50gaPymqM7R8tB6FBP/mUybI6BabNqDhQbsMgz+snEEPUdWy2rPfJC4Q9ZPGU2\nUu9HL3djUqNs9Vuf+szss/Q6sUEZGVmHAqawHEEq84X8A9yD5PUBIH9+gOw3l5zg\n4JJlnBPnegOUq4m0jY0LFjDG7yFnqnO2RAy/dLjmnBBn8nCYB8fmCcRIrbzXk0Cy\noROW3ZCdEpKi3uA3ENxpwbBc5xxTQY39W4HUEUJomQMIMCozqPrKkqug/wAI5tno\nJCu5mfmZx+xa49qep1pT4yvjAMGuz9GlOVPv/uUN/1ymXAWWpsc4uVixPVm74J8u\ngV7+vfuhAgMBAAECggEADNO1qmFgcQh0C6sf8GnPfc//OsY/4eR9hZaPQS4hcHam\nAnLEA7mY2wkKwddlgVOX+X6Ax1xztYvjqVbdGRuEqMgS4r1oS+Tel4jCJjV8ZnJ1\nV3WfulVynrQc0oT0hD25TMU+FYPE43clB+bsxCzxkZl43mn+U/ieqG0GleonPis3\nTU1VbtTvFKQXZHiS3kmot0iZbUFtEAfxd9b4EUSf/Ftfez3SzM3hoW7PBG82ACZn\nIJk++XWrp8lXoAfa4YrPLytL8P61mPrim6cJRJ6iX3o86I8sPHx3WkevaryeaxT7\nxo6rUi/9AlBLtKMGSOA1Q8rOitkgjSNjhSSOc2KCSQKBgQDt+jcDY8GoBAeYVZBA\nBPyVoTwoZKrIuzpPuDqABMRSLznfgUk44w7tYFjFTSZlVduREOkdu+/B8Y7/GEWc\no4jGKrhLsFxpZHZXBmZPSxFh1fB820C449o9CR4mt0PWyaxp9SxTIGgGqQtK8aKg\nMxEVfRgNKAG5iaNwWiBYIh1GCQKBgQDhUrr732ldsgkHddpl6K8CJTVojTcWPUbl\n0De/Ob3ildmKDZ+yh9CRJYcXrke1JBNDScpzrU+Oc/Yo0GIH9+DyU9YgOvgKcWlG\nGUA00HPQlXgxfKZn6Kq6S5q5msGm3Jjej0KNSU55ZpWdomcZqiIgsJ1DXIodHx1i\nFIkcXJgu2QKBgFXmQ4VNtNFY4wTxnaf+JM497OEHtT3PlzS/Yx5imhqwQUuT90I5\nc2HrrR9KwdlCLIcoe3zoIZr3/o3FzRrFTNF7ChsIBdNlF/JBtRxaFGQkPTJUUgBq\n/pOZfvCqpioZkcqNXPqEcAg3bIQYxILe7AO9q7jUAAtgy0X6wkINzlNBAoGAc9/e\nxv7aqdOP7yU9fmEN9zV5ZN4ScT5sAm107cNdEnRrniJpeR99M9evxI7c05vMyDLf\niWOcYS7jbZPUhjKEnTpeCM8qlfxC3kjICyEUHjVvzUtELHWrrwiIdmDOq+gQOYxE\n9nG3iJkn4hwNl1Y2HMeW6HhjkpxYuQLEjDBWFukCgYEAoA6ZmrfMof0bkHkBq1dv\n9Ea4X51qqyl9sts3jbGOwkR5XVEnPUdUCVjSyYpnrGyL+xLzNq66oL8xnUuVfHcd\ngMR+WIVOUeHKMyQX7NgFfiTqyCdJjxVELFLgNZS1IDfL3c5paktpfLMahY+J4/s2\n6H4p/6OJ5rnCZUxq8rqPsZ4=\n-----END PRIVATE KEY-----\n",
        "client_email": "pap-user-387@paris-air-pollution-quangnc.iam.gserviceaccount.com",
        "client_id": "118172829759259974410",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pap-user-387%40paris-air-pollution-quangnc.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }  # enter your credentials from the json file
)
credentials_block.save("paris-air-pollution-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("paris-air-pollution-gcp-creds"),
    bucket="airpollution_paris-air-pollution-quangnc",  # insert your  GCS bucket name
)

bucket_block.save("paris-air-pollution-gcs", overwrite=True)
