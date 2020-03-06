#!/bin/bash

set -e

function print_usage(){
  echo "Usage: hcs-git-cmd.sh COMMAND [optional:메시지]"
  echo "       where COMMAND is one of:"
  echo "  status               커밋되지 않은 변경사항 리스트 조회"
  echo "  add                  변경사항들 Staging 영역에 올리기"
  echo "  commit               변경사항들 로컬저장소에 커밋하기, 커밋을 할때 메시지도 함께 넘김 "
  echo "           예) hcs-git-cmd.sh commit \"메시지\""
  echo "  push                 원격저장소에 올리기"
  echo "  pull                 원격저장소에서 받기"
  echo ""
  echo "Most commands print help when invoked w/o parameters."
}

if [ $# = 0 ]; then
  print_usage
  exit 1
fi

echo "*** push 하기전에 먼저 pull 받기 !!! "

cd ./HCS-project

COMMAND=$1
case $COMMAND in
  # usage flags
  --help|-help|-h)
    print_usage
    exit
    ;;

  #status
  status)
    git status
    ;;

  #add
  add)
    git add *
    ;;

  #commit
  commit)
    if [ -z "$2" ]; then
      echo "예) hcs-git-cmd.sh commit \"메시지\""
      exit 1;
    fi

    git commit -m $2
    ;;

  #push
  push)
    git pull

    git push
    ;;

  #pull
  pull)
    git pull
    ;;

esac

