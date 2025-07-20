# Database Comparison Report

**Generated:** 2025-07-20T09:29:22.243102

## Summary
- **Total Tables:** 2
- **Identical Tables:** 0
- **Tables with Differences:** 2
- **Total Rows Compared:** 10
- **Total Differences Found:** 6

## Data Differences

### Table: users

- **Row Count DB1:** 3
- **Row Count DB2:** 3
- **Matching Rows:** 1
- **Rows Only in DB1:** 2
- **Rows Only in DB2:** 2
- **Rows with Differences:** 0

#### Rows Only in Database 1

- Row 1: {'id': 'uuid-user1', 'username': 'john_doe', 'email': 'john@example.com', 'created_at': '2024-01-01 10:00:00', 'is_active': 1}
- Row 2: {'id': 'uuid-user2', 'username': 'jane_smith', 'email': 'jane@example.com', 'created_at': '2024-01-02 11:00:00', 'is_active': 1}

#### Rows Only in Database 2

- Row 1: {'id': 'uuid-user1-different', 'username': 'john_doe', 'email': 'john.doe@example.com', 'created_at': '2024-01-01 10:00:00', 'is_active': 0}
- Row 2: {'id': 'uuid-user2', 'username': 'jane_doe', 'email': 'jane@example.com', 'created_at': '2024-01-02 11:00:00', 'is_active': 1}

### Table: posts

- **Row Count DB1:** 2
- **Row Count DB2:** 2
- **Matching Rows:** 1
- **Rows Only in DB1:** 1
- **Rows Only in DB2:** 1
- **Rows with Differences:** 0

#### Rows Only in Database 1

- Row 1: {'id': 'uuid-post1', 'user_id': 'uuid-user1', 'title': 'First Post', 'content': 'This is the first post content', 'created_at': '2024-01-01 15:00:00'}

#### Rows Only in Database 2

- Row 1: {'id': 'uuid-post1', 'user_id': 'uuid-user1', 'title': 'Modified First Post', 'content': 'This content has been completely changed!', 'created_at': '2024-01-01 15:00:00'}
