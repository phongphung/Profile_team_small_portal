__author__ = 'sunary'


import json
from linkedin import linkedin


class LinkedinService():

    def __init__(self):
        self.list_key = {'app_key': '75uwaap0llsimm',
                         'app_key_secret': 'De6QkKePsBJPLv4i'}

        self.data_fields = ['id', 'first-name', 'last-name', 'maiden-name', 'formatted-name', 'phonetic-first-name', 'phonetic-last-name',
                       'formatted-phonetic-name', 'headline', 'location', 'industry', 'current-share', 'num-connections',
                       'num-connections-capped', 'summary', 'specialties', 'positions', 'picture-url', 'picture-urls',
                       'site-standard-profile-request', 'api-standard-profile-request', 'public-profile-url']

    def get_profile(self, user_id):
        authentication = linkedin.LinkedInDeveloperAuthentication(
                        self.list_key['app_key'],
                        self.list_key['app_key_secret'],
                        user_id['token'],
                        user_id['token_secret'],
                        'http://localhost:8000',
                        linkedin.PERMISSIONS.enums.values())

        application = linkedin.LinkedInApplication(authentication)
        print json.dumps(application.get_profile(selectors= self.data_fields), ensure_ascii= False, indent=4, sort_keys=True)

    def get_profile_v2(self, user_id):
        application = linkedin.LinkedInApplication(token= user_id['token'])
        print json.dumps(application.get_profile(selectors= self.data_fields), ensure_ascii= False, indent=4, sort_keys=True)

if __name__ == '__main__':
    linkedin_service = LinkedinService()
    linkedin_service.get_profile({'token': '37436bd4-c1d9-48f5-98c0-e337a4081dfe', 'token_secret': '2fde484d-c897-4183-b9b7-5f8cb0ac8928'})
    linkedin_service.get_profile_v2({
        'token': 'AQXxqU1dXuyPYULbJzwDKwPwl3Ji5d2UHektCt9HPNGUsAWAMiBEjIzbtZ4FNGo1kb1WGo4Z9oteswhJ7lYLU_EOgIKyKPnNmgB5CmlvMTvinPbAYT5RlHFowE2TEKoNmtitCz-pgVTeegRSpUn2kmGfiA-_kVJP00CFai-eN7Ak9iW44Ok'})
    linkedin_service.get_profile_v2({
        'token': 'AQWu2KyIDGWL9Kax7CXQfWPGmkuY66DpPlF7QCcgIcLS3jBG32KUusGBGr8NCp0Q9PVR65fZaZBiy5IMBO7OSBI-uQLbaOcpZ9GwJrBlRTpdDh8VPL_mkRaSvJ25sPmbz_N858U4T9BAbfRsGMaNM74dD9xmbHrIpA47W5QPtWkfwFByTj4'})
    linkedin_service.get_profile_v2({
        'token': 'AQUPGiqRkmFuErSgeeX_yYoGpJo5oAJSZOPx0S-r2r-dKwkjN23D60zT0q5L1aEk_HmH2V2MC5T86NBkwA2msMp5GiYq4BuqmD0q0mGkFgdgG6u1kuUERIZC6WeJEPX8T8rVWj9dpa9I_5Rq9GJ9Xg4kdEnnmJ2DfrRXcf4FL9NcE8ajeUQ'})
    linkedin_service.get_profile_v2({
        'token': 'AQUhhN9-t7xjQm2PHZaPDsV6Z8l0R5nVmpZI6MP4Meul1whcPJoJOTH5TRg7NUuidmQ09UkukWKxbNLhS0a5bUtFjZ4mg7D1XlGtwXCNI4hKCJFD_rj2KYw81zcOh2sxeMsJ4ZCohaSzzqvENNERiO2Pd4ouA4BkEdbqpRtolzVHNj61UKY'})