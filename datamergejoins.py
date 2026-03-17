import pandas as pd
print("TASK 1\n")
students_data = {
    'student_id': [101, 102, 103, 104, 105, 106, 107],
    'name': ['Alice', 'Bob', None, 'David', 'Emma', 'Frank', 'Grace'],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', None, 'emma@email.com', 'frank@email.com', 'grace@email.com'],
    'city': ['Mumbai', 'Delhi', 'Bangalore', 'Mumbai', None, 'Chennai', 'Delhi']
}
enrollments_data = {
    'student_id': [101, 102, 103, 105, 108, 109],
    'course_name': ['Python', 'Data Science', 'Python', 'Machine Learning', 'AI', 'Python'],
    'enrollment_date': ['2024-01-15', '2024-01-20', '2024-02-01', '2024-02-10', '2024-02-15', '2024-03-01']
}
scores_data = {
    'student_id': [101, 102, 104, 105, 106],
    'exam_score': [85, 92, 78, 88, 95]
}

students_df = pd.DataFrame(students_data)
enrollments_df = pd.DataFrame(enrollments_data)
scores_df = pd.DataFrame(scores_data)
print("Original Students DataFrame:")
print(students_df)

print("\nNull Value Analysis:")
for col in students_df.columns:
    nulls = students_df[col].isnull().sum()
    percent = (nulls / len(students_df)) * 100
    print(f"Column: {col}, Nulls: {nulls} ({percent:.2f}%)")

students_df['city'].fillna('Unknown', inplace=True)
students_df = students_df.dropna(subset=['name'])
print(f"\nCleaned Students DataFrame ({len(students_df)} rows):")
print(students_df)

print("\n=== TASK 2\n")

inner_join = pd.merge(students_df, enrollments_df, on='student_id', how='inner')
print(f"Inner Join Result ({len(inner_join)} rows):")
print(inner_join[['student_id', 'name', 'course_name', 'enrollment_date']])
excluded = students_df[~students_df['student_id'].isin(enrollments_df['student_id'])]['student_id'].tolist()
print(f"Excluded students: {excluded} - Not in enrollments table\n")

left_join = pd.merge(students_df, enrollments_df, on='student_id', how='left')
print(f"Left Join Result ({len(left_join)} rows):")
print(left_join[['student_id', 'name', 'course_name', 'enrollment_date']])
null_courses = left_join[left_join['course_name'].isnull()]['student_id'].tolist()
print(f"Students with null course_name: {null_courses}\n")

right_join = pd.merge(students_df, enrollments_df, on='student_id', how='right')
print(f"Right Join Result ({len(right_join)} rows):")
print(right_join[['student_id', 'name', 'course_name', 'enrollment_date']])

outer_join = pd.merge(students_df, enrollments_df, on='student_id', how='outer')
print(f"\nOuter Join Result ({len(outer_join)} rows):")
print(outer_join[['student_id', 'name', 'course_name', 'enrollment_date']])

print("\n=== TASK 3\n")

score_dict = dict(zip(scores_df['student_id'], scores_df['exam_score']))
students_df['exam_score'] = students_df['student_id'].map(score_dict)
print("Lookup Operation Result:")
print(students_df[['student_id', 'name', 'exam_score']])

def auto_merge(df1, df2, join_type, key_column):
    merged_df = pd.merge(df1, df2, on=key_column, how=join_type)
    return {
        'result_df': merged_df,
        'row_count': len(merged_df),
        'join_type': join_type
    }

test_inner = auto_merge(students_df, enrollments_df, 'inner', 'student_id')
print("\nAutomation Function Test:")
print(f"Join Type: {test_inner['join_type']}")
print(f"Rows in Result: {test_inner['row_count']}")
print("Result Preview:")
print(test_inner['result_df'][['student_id', 'name', 'course_name']])