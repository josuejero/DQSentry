"""Defines the raw template rows used by the synthetic generator."""

DISTRICT_TEMPLATES = [
    {"key": "nyc", "name": "NYC", "state": " ny "},
    {"key": "northside", "name": "  northside unified", "state": "Ca"},
    {"key": "northside", "name": "Northside Unified", "state": "CA"},
    {"key": "southvalley", "name": "South Valley", "state": "Texas"},
    {"key": "sunrise", "name": "Sunrise Charter", "state": " CAX "},
]

USER_TEMPLATES = [
    {
        "key": "alice",
        "email": "  Alice.Smith@Example.com ",
        "org": "Mastery Labs",
        "role": "teacher",
        "state": "ca",
        "district": "northside",
    },
    {
        "key": "bob",
        "email": "bob@example.COM",
        "org": "NYC Elevate",
        "role": "district_admin",
        "state": "NY",
        "district": "nyc",
    },
    {
        "key": "carol",
        "email": "",
        "org": "Northside Alliance",
        "role": "student",
        "state": "California",
        "district": "northside",
    },
    {
        "key": "dax",
        "email": " Dax@Learning.org  ",
        "org": "Mastery Labs",
        "role": "teacher",
        "state": "tx",
        "district": "southvalley",
    },
    {
        "key": "eve",
        "email": "EVE.lem@Example.com",
        "org": "Sunrise Initiative",
        "role": "other",
        "state": " cax ",
        "district": None,
    },
    {
        "key": "frank",
        "email": " frank@example.com",
        "org": "Sunrise Initiative",
        "role": "admin",
        "state": "zz",
        "district": "southvalley",
    },
    {
        "key": "glenda",
        "email": "GLENDA@Example.Com",
        "org": "NYC Elevate",
        "role": "teacher",
        "state": "nyc",
        "district": "nyc",
    },
    {
        "key": "alice_alt",
        "email": "ALICE.Smith@example.com",
        "org": "Mastery Labs",
        "role": "teacher",
        "state": "ca",
        "district": "northside",
    },
]

RESOURCE_TEMPLATES = [
    {
        "key": "math-fundamentals",
        "type": "lesson",
        "subject": "  Math ",
        "grade_band": "grades pre k to 2",
    },
    {
        "key": "capstone-think",
        "type": "Assessment",
        "subject": "ELA",
        "grade_band": "Grades 3-5",
    },
    {
        "key": "science-explorers",
        "type": "ACTIVITY",
        "subject": " science",
        "grade_band": "Grades 6-8 ",
    },
    {
        "key": "pathway-start",
        "type": "module ",
        "subject": "Social Studies",
        "grade_band": "High School",
    },
    {
        "key": "math-fundamentals",
        "type": "lesson",
        "subject": "math",
        "grade_band": "PreK-2",
    },
    {
        "key": "missing-resource",
        "type": "module",
        "subject": "Unknown",
        "grade_band": "9-12",
    },
]

EVENT_TEMPLATES = [
    {
        "key": "evt-view",
        "user": "alice",
        "resource": "math-fundamentals",
        "event_type": " view ",
        "event_ts": "2024-01-15 14:30:00",
    },
    {
        "key": "evt-start",
        "user": "bob",
        "resource": "capstone-think",
        "event_type": "Start",
        "event_ts": "01/06/2024 08:00:00",
    },
    {
        "key": "evt-start-repeat",
        "user": "bob",
        "resource": "capstone-think",
        "event_type": "start",
        "event_ts": "01/06/2024 08:00 AM",
    },
    {
        "key": "evt-complete",
        "user": "carol",
        "resource": "science-explorers",
        "event_type": "complete",
        "event_ts": "March 3 2024 16:35",
    },
    {
        "key": "evt-orphan",
        "user": "glenda",
        "resource": "missing-resource",
        "event_type": "submit",
        "event_ts": "2024-03-05T09:15:00+00:00",
    },
    {
        "key": "evt-share",
        "user": "frank",
        "resource": "pathway-start",
        "event_type": "share",
        "event_ts": "04/02/2024 13:20",
    },
]

NEWSLETTER_TEMPLATES = [
    {
        "email": "  Alice.Smith@Example.com",
        "subscribed_at": "2024-01-01T09:00:00Z",
        "opened_at": "2024-01-02 10:00 AM",
        "clicked_at": "",
    },
    {
        "email": "bob@example.com",
        "subscribed_at": "1/5/2024 08:00",
        "opened_at": "2024-01-06T09:30:00+00:00",
        "clicked_at": "Jan 7 2024 10:00",
    },
    {
        "email": "",
        "subscribed_at": "2024-01-08 11:11:11",
        "opened_at": "",
        "clicked_at": "",
    },
    {
        "email": "bob@example.com",
        "subscribed_at": "2024-01-05 08:00:00",
        "opened_at": "2024-01-06 09:30",
        "clicked_at": "2024-01-07 10:00",
    },
]
