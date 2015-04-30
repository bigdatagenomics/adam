# This script is shamelessly taken from the Parquet team @ https://github.com/Parquet/parquet-mr

echo "github username:" >&2 
read username >&2
echo "github password:" >&2 
read -s password >&2

curl -f -u $username:$password -s "https://api.github.com" > /dev/null
if [ $? == 0 ]
then
  echo "login successful" >&2
else
  echo "login failed" >&2
  curl -u $username:$password -s "https://api.github.com"
  exit 1
fi

echo "# ADAM #"

git log | grep -E "Merge pull request|prepare release" | grep -vi "Revert" | uniq | while read l
do 
  release=`echo $l | grep "prepare release" | grep -v 2.11 | awk -F'-' '{print $NF}' | awk -F'_' '{ print $1 }'`
  PR=`echo $l| grep -E -o "Merge pull request #[^ ]*" | cut -d "#" -f 2`
#  echo $l
  if [ -n "$release" ] 
  then 
    echo
    echo "### Version $release ###"
  fi
  if [ -n "$PR" ]
  then
    JSON=`curl -u $username:$password -s https://api.github.com/repos/bigdatagenomics/adam/pulls/$PR | tr "\n" " "`
    DESC_RAW=$(echo $JSON | egrep -o '"title":.*?[^\\]",' | cut -d "\"" -f 4- | head -n 1 | sed -e "s/\\\\//g")
    DESC=$(echo ${DESC_RAW%\",})
    echo "* ISSUE [$PR](https://github.com/bigdatagenomics/adam/pull/$PR): ${DESC}"
  fi
done
