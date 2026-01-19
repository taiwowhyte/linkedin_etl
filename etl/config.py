"""
Schema-level configuration for transformations.

- column wanted list
- varchar limits
- natural keys list
"""
WANTED_COLUMNS = {
    'job_postings_staging':['job_link','job_title','company', 'job_location','search_city', 'search_country', 'search_position', 'job_level',
       'job_type', 'job_summary', 'job_skills'],
       'job_postings_skills_staging':['job_skills','job_link'],
       'skills':['job_skills'],
       'search_context':['search_country','search_city','search_position'],
       'location':['job_location']



}

VAR_CHAR_LIMITS = {'job_postings_staging':{'job_link':2048},
                   #must set to {} as None causes error
                   'job_postings_skills_staging':{},
                   'search_context':{'search_country':255, 'search_city':255,'search_position':255},
                   'job_title':{'canonical_job_title':255},
                   'location':{'job_location':255},
                   'company':{'company':255},
                   'company_w_unknown':{'company':255,
                                     'company_plus_unknown':255}
}


NATURAL_KEY_COLUMNS = {'job_postings_staging': ['job_link'],
                       'job_postings_skills_staging':['job_skills','job_link'],
                       'search_context':['search_country','search_city','search_position'],
                       'location':['job_location']}
