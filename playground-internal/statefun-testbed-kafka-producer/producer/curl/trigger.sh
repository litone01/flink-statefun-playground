# clear the previous outputs
rm -f *.err *.out
sleep 1

./s1.sh > s1.out 2> s1.err &
echo "stream 1 started"
./s2.sh > s2.out 2> s2.err &
echo "stream 2 started"