#!/bin/bash

# Kill any servers already running on these ports
lsof -ti:2000,2001,2002 | xargs kill -9 2>/dev/null

# Start all three servers simultaneously in new terminal tabs
osascript -e 'tell application "Terminal"
    do script "cd '$PWD' && go run raftserver/raftserver.go localhost:2000 servers.txt"
    do script "cd '$PWD' && go run raftserver/raftserver.go localhost:2001 servers.txt"
    do script "cd '$PWD' && go run raftserver/raftserver.go localhost:2002 servers.txt"
end tell'