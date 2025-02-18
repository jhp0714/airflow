FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "you selected apple!"
elif [ $FRUIT == ORANGE ];then
	echo "you selected orange!"
elif [ $FRUIT == GRAPE ];then
	echo "you selected grape!"
else
	echo "you selected other fruit!"
fi
