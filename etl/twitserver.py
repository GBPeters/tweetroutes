__author__ = 'gijspeters'

from twython import TwythonStreamer

API_KEY = ['nNJW0Htrw5hzFYoKljPUlKBzk',
           '4S5lDPYp6kbdGTTQ34M1pcLsk']
API_SECRET = ['9AoAc8YwguVy6YLHEAcum2Ho1nlRyrK5fRsnOkgwNV1Zd8AbdT',
              '2TsPa6xziXfFHdRpLfR1S9CyxGbhTQWhzxI1UqRW3fdaaZAneo']
TOKEN = ['3247956964-ov5UMweNO0l1ayQVcgc2HxkJzCu7UUbAT3TnGA9',
         '3247956964-DCFmG6nXbF91BdjFmxPEpMVuj61e50nfVRLXJv2']
TOKEN_SECRET = ['oO5ZBP9yp9Klg3isi3OXEC1O7ymgv0TyLMjdkxRPYHTox',
                'hCH4mjEgVEPBmfSNlPDlsyklNOldBDXMtW9jOHVvLG9RJ']

class TwitStreamer (TwythonStreamer):

    def on_success(self, data):
        if 'text' in data:
            print data['created_at'].encode('utf-8') + ' - ' + data['user']['screen_name'].encode('utf-8')+ ': ' + data['text'].encode('utf-8')

    def on_error(self, status_code, data):
        print status_code
        self.disconnect()
       # self.