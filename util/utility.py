class HDFS:
  def __init__(self, host, port=50070, use_https=False, secure=False, **kwds):
    import requests

    session = kwds.setdefault('session', requests.Session())
    session.verify = True

    prefix = 'https' if use_https else 'http'
    url = '{0}://{1}:{2}'.format(prefix, host, port)
    
    if secure:
      import requests_kerberos
      from hdfs.ext.kerberos import KerberosClient
      kwds.setdefault('mutual_auth', 'OPTIONAL')
      
      self.client = KerberosClient(url, **kwds)
    
    else:
      from hdfs.client import InsecureClient
      
      self.client = InsecureClient(url, **kwds)

  def exists(self, path):
    return bool(self.client.status(path, strict=False))