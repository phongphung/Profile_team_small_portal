__author__ = 'sunary'


from facepy import GraphAPI


class FacebookService():

    def __init__(self):
        pass

    def get_profile(self, access_token):
        graph = GraphAPI(oauth_token=access_token)
        profile = graph.get(path='me??fields=last_name,first_name,middle_name,name,email,address,birthday,hometown,location,gender,age_range')
        print profile

if __name__ == '__main__':
    facebook = FacebookService()
    facebook.get_profile('CAAGiXNM0o0wBAD8LadoYEn6wTr5Ws2oAICJXHgZBqgQUTdvDBtWUgBHRUADiXPtylY4mEnUvDzzEP4lVEePTC5vu1qYuTFNY7hBBFoxuYgwuTqIO3IbjkBTg11D5ypwBK3gLqk0DvTIteoPxNSH3OIHH0ExYs3ZBaWv5kRm4xZBXMSs1cNsqW7iF3ehKs8b3xZCrubHTT4swZApc6zejN')

