# clear the previous outputs
rm -f *.err *.out
sleep 1

./s1_producer.sh > s1_producer.out 2> s1_producer.err &
echo "stream 1 started"
./s2_producer.sh > s2_producer.out 2> s2_producer.err &
echo "stream 2 started"

sleep 1
./s1_consumer.sh > s1_consumer.out 2> s1_consumer.err &
echo "stream 1 consumer started"
./s2_consumer.sh > s2_consumer.out 2> s2_consumer.err &
echo "stream 2 consumer started"

