# Simple code snippet for testing web and API
The Python scripts allow you to mock connecting to a website or api

---
## Setup http with auth

```
export BASIC_USER="ryan"
export BASIC_PASS="supersecret"
python3 auth_http_server.py 8000
```
---
## Testing API
Then visit http://localhost:8000 and your browser will prompt for credentials.



---
## Setup API

```
export BASIC_USER="ryan"
export BASIC_PASS="supersecret"
python3 auth_api_server.py 8000
```
---
## Testing API

1. **Clone or download this repository.**
2. **Create a `.env` file in the project root** with the following content:
```
curl -u ryan:supersecret http://localhost:8000/api/info
curl -u ryan:supersecret -H "Content-Type: application/json" -d '{"ping":"pong"}' http://localhost:8000/api/echo
```
---


## Notes:
- This is Basic Auth and not encryption
- Will show the files in the current path

---
## Author
Ryan Gillan  
Email: ryangillan@gmail.com
