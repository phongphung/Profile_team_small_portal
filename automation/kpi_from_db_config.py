

# daily messages report
ID_TWEET_ORANGE_FLOW = '1_orange_flow'
ID_TWEET_REAL_TIME = '1_real_time'
ID_CANDIDATES_PROCESSED = '1_candidated_processed'
ID_PROCESSING_MESSAGES = '1_processing_messages'

# daily qualified report
ID_TOTAL_CANDIDATES = '1_total_candidates'
ID_TOTAL_PROCESSED = '1_total_processed'
ID_TOTAL_EXPORTED = '1_total_exported'
ID_TOTAL_CLASSIFIED = '1_total_classified'
ID_TOTAL_QUALIFIED = '1_total_qualified'
ID_TOTAL_DISQUALIFIED = '1_total_disqualified'

ID_QUALIFIED_STATUSES = '1_qualified_statuses'
QUALIFIED_STATUSES = [{'status': 8, 'name': 'Profession'},
                    {'status': 9, 'name': 'CR'},
                    {'status': 10, 'name': 'Named Entities'},
                    {'status': 11, 'name': 'Guru Following'},
                    {'status': 12, 'name': 'Seed Following'},
                    {'status': 13, 'name': 'Following Seed'},
                    {'status': 14, 'name': 'Cashtags'},
                    {'status': 15, 'name': 'Guru Mention'},
                    {'status': 16, 'name': 'Sentifi Mention'},]

# breakdown statuses
ID1_GNIP_BREAKDOWN = '1_gnip_breakdown'
ID1_STREAM_BREAKDOWN = '1_stream_breakdown'
ID1_SEED_BREAKDOWN = '1_seed_breakdown'
ID1_MENTION_BREAKDOWN = '1_mention_breakdown'

ID7_GNIP_BREAKDOWN = '7_gnip_breakdown'
ID7_STREAM_BREAKDOWN = '7_stream_breakdown'
ID7_SEED_BREAKDOWN = '7_seed_breakdown'
ID7_MENTION_BREAKDOWN = '7_mention_breakdown'

#change profile twitter
ID_NUM_CHANGE_DES = '1_num_change_des'
ID_NUM_CHANGE_AVATAR = '1_num_change_avatar'
ID_NUM_CHANGE_LOC = '1_num_change_loc'
ID_NUM_CHANGE_NAME = '1_num_change_name'
ID_NUM_CHANGE_URL = '1_num_change_url'
ID_NUM_CHANGE_ANY = '1_num_change_any'

# weekly source report
ID_SOURCE_1DES_1URL = '7_source_1des_1url'
ID_SOURCE_1DES_0URL = '7_source_1des_0url'
ID_SOURCE_0DES_1URL = '7_source_0des_1url'
ID_SOURCE_0DES_0URL = '7_source_0des_0url'
ID_SOURCE_0DES_GT200 = '7_source_0des_gt200'
ID_SOURCE_0DES_GTE100_LT200 = '7_source_0des_gte100_lt200'
ID_SOURCE_0DES_LT100 = '7_source_0des_lt100'

# weekly publisher report
ID_TWITTER_BROKEN_AVATAR = '7_twitter_broken_avatar'
ID_TWITTER_MISSING_AVATAR = '7_twitter_missing_avatar'
ID_TWITTER_MISSING_ISO = '7_twitter_missing_iso'
ID_TWITTER_HAS_MANY_ITEMKEY = '7_twitter_has_many_itemkey'
ID_TWITTER_HAS_MANY_PUBLISHER = '7_twitter_has_many_publisher'
ID_TWITTER_WITHOUT_ITEMKEY = '7_twitter_without_itemkey'

# publisher snapshot
ID_PUBLISHER_SNAPSHOT = '7_publisher_snapshot'

# publisher qualified
ID_PUBLISHER_QUALIFIED_ISOCODE = '7_publisher_qualified_isocode'

#publisher classified
CATEGORIES = ['Accountant', 'Activist', 'Actuary', 'Administrator', 'Advisor', 'Analyst', 'AngelInvestor', 'Appraiser', 'Architect', 'Artist', 'AssetManager', 'Assistant', 'AssistantProfessor', 'Associate', 'Athlete', 'Attorney', 'Auditor',
            'Banker', 'BankTeller', 'BoardAdvisor', 'BoardChair', 'BoardDirector', 'BoardGeneralMember', 'BoardMember', 'BoardPresident', 'BoardSecretary', 'BoardTreasury', 'BoardViceChair', 'BoardVP', 'Bookkeeper', 'Broker', 'BusinessFinanceJournalist', 'BuySideAnalyst',
            'CEO', 'CEOFinOrg', 'CFO', 'ChiefAcademic', 'ChiefAccounting', 'ChiefAdmin', 'ChiefAnalytics', 'ChiefAudit', 'ChiefBrand', 'ChiefBusiness', 'ChiefCommercial', 'ChiefComms', 'ChiefCompliance', 'ChiefContent', 'ChiefCreative', 'ChiefCredit', 'ChiefCustomer', 'ChiefData', 'ChiefDesign', 'ChiefDigital', 'ChiefDiversity', 'ChiefEngineering', 'ChiefExperience', 'ChiefHR', 'ChiefInfoOfficer', 'ChiefInfoSecurity', 'ChiefInnovation', 'ChiefInsurance', 'ChiefIP', 'ChiefIntl', 'ChiefInvestment', 'ChiefInvestorRelationsOfficer', 'ChiefKnowledge', 'ChiefLearning', 'ChiefLending', 'ChiefMarketing', 'ChiefMedia', 'ChiefMedical', 'ChiefNetworking', 'ChiefofStaff', 'ChiefOperatingOfficer', 'ChiefPerformance', 'ChiefPrivacy', 'ChiefProcurement', 'ChiefProduct', 'ChiefProgram', 'ChiefQuality', 'ChiefRelationship', 'ChiefResearch', 'ChiefRevenue', 'ChiefRiskOfficer', 'ChiefSales', 'ChiefScience', 'ChiefSecurity', 'ChiefStrategy', 'ChiefSustainability', 'ChiefTax', 'ChiefTechnology', 'ChiefVisionary', 'Controller', 'Coordinator', 'CorporateCommunication', 'CostEstimator', 'CountryAmbassador', 'CountryMinister', 'CountryPresident', 'CustomerServiceRepresentative',
            'Designer', 'Developer',
            'Economist', 'EditorChief', 'EducationalProfessional', 'EducationalProfessionalBusinessFinance', 'EducationalProfessionalOther', 'EducationalProfessionalTechnology', 'Educator', 'Employee', 'Engineer', 'Entrepreneur', 'ExecManagement', 'Executives', 'Expert', 'ExternalAuditor',
            'FinancialAdvisor', 'FinancialAdvisorOther', 'FinAnalyst', 'FinancialAnalyst', 'FinancialAnalystOther', 'FinancialEngineer', 'FinExecutive', 'FinExpert', 'FinJournalist', 'FinancialMarketBroker', 'FinMarketingProfessional', 'FinancialMarketProfessionals', 'FinMarketTrader', 'FinancialPlanner', 'FinancialProfessionalOther', 'FinancialProfessionalOtherChild', 'FinancialResearcher', 'FinancialSpecialist', 'FinancialTechnician', 'Founder', 'FuncRole', 'FunctionalChief', 'FunctionalDirector', 'FunctionalManager', 'FunctionalSupervisor', 'FundPortfolioManager', 'Fundraiser',
            'GeneralCounsel', 'GovSecretary',
            'Instructor', 'InternalAuditor', 'InvestmentAdvisor', 'InvestmentBanker', 'InvestorRelationsProfessional',
            'JobRecruiter', 'Journalist', 'Judge',
            'Lawyer', 'Lecturer', 'Librarian', 'LoanOfficer', 'Logistician',
            'MarketingProfessional', 'MiddleManagement', 'Minister', 'MultiAssetClassTrader',
            'NewsJournalist', 'NonFinancialExecutive', 'NonidentifiedPersons', 'NonNewsJournalist',
            'Occupation', 'OtherAdvisor', 'OtherAnalyst', 'OtherBanker', 'OtherBroker', 'OtherEngineer', 'OtherExecutiveFinOrg', 'OtherExpert', 'OtherFinAdvisor', 'OtherMarketingProfessional', 'OtherPerson', 'OtherResearcher', 'OtherSpecialist', 'OtherStakeholders', 'OtherTechnician', 'Owner',
            'Partner', 'Person', 'Physician', 'Politician', 'Practitioner', 'PrimeMinister', 'PrivateEquityProf', 'PrivateInvestor', 'PrivateSecRole', 'Producer', 'Profession', 'Professor', 'PublicSecRole',
            'Receptionist', 'ReligiousSecRole', 'Researcher', 'RetailBanker', 'RiskAdvisor', 'RiskAnalyst', 'RiskManager', 'RiskProfessional', 'Root',
            'SalesMarketingFinOrg', 'Scientist', 'Secretary', 'SellSideAnalyst', 'Specialist', 'Spokesperson', 'Staff', 'Staffs', 'Student', 'Stylist', 'Surveyor',
            'Teacher', 'Technician', 'Trader', 'TraderOther', 'TradingAdvisor', 'Treasurer',
            'Underwriter', 'Unionist',
            'VentureCapitalProf', 'VicePresident', 'Volunteer',
            'Worker']

SELECTED_ISOCODE = ('au', 'ca', 'ch', 'cn', 'de', 'gb', 'hk', 'in', 'sa', 'sg', 'us')


