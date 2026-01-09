# feasibility_spike.py
# Simple skill-based job matching prototype

jobs = {
    "Backend Engineer": {"python", "sql", "apis", "databases"},
    "Data Analyst": {"sql", "python", "statistics", "excel"},
}

users = {
    "alice": {"python", "sql", "apis"},
    "bob": {"excel", "statistics"},
}

def match_score(user_skills, job_skills):
    return len(user_skills & job_skills) / len(job_skills)

for user, skills in users.items():
    for job, reqs in jobs.items():
        score = match_score(skills, reqs)
        print(f"{user} → {job}: {score:.2f}")
