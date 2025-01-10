import random
import uuid
import os
import json
from datetime import datetime, timedelta


def generate_course_codes():
    random.seed(42) 

    course_numbers = random.sample(range(100, 1000), 60)
    course_prefixes = ['CS', 'CL', 'CM','MA','SC', 'HI', 'PH', 'BI', 'EC', 'PS', 'EN', 'GE', 'AR', 'LA', 'PE']

    course_codes = []

    for prefix in course_prefixes:
        prefix_numbers = course_numbers[:4]
        course_numbers = course_numbers[4:]

        for number in prefix_numbers:
            course_codes.append(f"{prefix}{number}")
    return course_codes

def generate_student_ids(num_students=60):

    return [f"S{random.randint(10000, 99999)}" for _ in range(num_students)]

course_codes = generate_course_codes()
student_ids = generate_student_ids()

random.seed(None)

def generate_timestamp():

    evening_start = datetime.now().replace(hour=17, minute=0, second=0)
    evening_end = datetime.now().replace(hour=22, minute=0, second=0)

    normal_hours_start = datetime.now().replace(hour=8, minute=0, second=0)
    normal_hours_end = datetime.now().replace(hour=16, minute=0, second=0)

    if random.random() < 0.6:
        timestamp = evening_start + timedelta(seconds=random.randint(0, (evening_end - evening_start).seconds))
    else:
        timestamp = normal_hours_start + timedelta(seconds=random.randint(0, (normal_hours_end - normal_hours_start).seconds))

    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

course_categories = {
    "technical": ["CS", "CL", "CM", "MA", "EC", "BI"],  
    "humanities": ["HI", "PH", "EN", "AR", "LA"],        
    "science": ["SC", "PS"],                             
    "arts": ["PE", "GE"],                                 
}

def categorize_course(course_code):
    for category, prefixes in course_categories.items():
        if any(course_code.startswith(prefix) for prefix in prefixes):
            return category
    return "others" 

def choose_course(student_id):
    all_courses = {
        "technical": [course for course in course_codes if categorize_course(course) == "technical"],
        "humanities": [course for course in course_codes if categorize_course(course) == "humanities"],
        "science": [course for course in course_codes if categorize_course(course) == "science"],
        "arts": [course for course in course_codes if categorize_course(course) == "arts"]
    }

    if int(student_id[1:]) % 3 == 0:

        course_code = random.choices(
            all_courses["technical"] + all_courses["humanities"] + all_courses["science"] + all_courses["arts"],
            weights=[0.6] * len(all_courses["technical"]) +
                    [0.1] * (len(all_courses["humanities"]) + len(all_courses["science"]) + len(all_courses["arts"])),
            k=1
        )[0]

    elif int(student_id[1:]) % 4 == 0:
      
        course_code = random.choices(
            all_courses["humanities"] + all_courses["technical"] + all_courses["science"] + all_courses["arts"],
            weights=[0.6] * len(all_courses["humanities"]) +
                    [0.1] * (len(all_courses["technical"]) + len(all_courses["science"]) + len(all_courses["arts"])),
            k=1
        )[0]
    else:
        course_code = random.choice(course_codes)

    return course_code

# print(course_codes)
# print(student_ids)


interaction_types = [
    "video_watched", "quiz_completed", "assignment_submitted", "forum_posted", "lesson_started",
    "lesson_completed", "certificate_earned", "course_started", "course_completed", "badge_earned",
    "course_reviewed", "video_liked", "comment_posted", "content_downloaded", "progress_shared",
    "practice_exam_taken", "feedback_submitted", "discussion_joined", "discussion_replied", "content_shared",
    "lesson_bookmarked", "profile_updated", "search_performed", "notification_read", "message_sent",
    "lesson_revisited", "course_rated", "content_added_to_playlist", "course_viewed"
]

with open('MOCK_DATA.json', 'r') as file:
    locations = json.load(file)


def generate_data():
    student_id = random.choice(student_ids)
    course_code = choose_course(student_id)
    interaction_type = random.choice(interaction_types)
    timestamp = generate_timestamp()
    duration = random.randint(20, 7200) #sec
    location = random.choice(locations) 

    data = {
        "student_id": student_id,
        "course_code": course_code,
        "interaction_type": interaction_type,
        "timestamp": generate_timestamp(),
        "duration": duration if interaction_type == "video_watched" else None,
        "location": {
            "country": location["country"],
            "city": location["city"],
        }
    }
    return data

data_batch = [generate_data() for _ in range(100)]

for data in data_batch:
    print(data)


