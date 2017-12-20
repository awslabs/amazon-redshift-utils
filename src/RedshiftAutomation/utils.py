CMK = 'alias/RedshiftUtilsLambdaRunner'

def get_encryption_context(region):
    authContext = {}
    authContext["module"] = CMK
    authContext["region"] = region
    
    return authContext