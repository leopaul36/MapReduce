//MapReduce Algorithm for HADOOP Class by Leo Schoukroun - ING5

//Map function
var map = function(line)
{
  //We split the received line into mutliple words in an array
  var array = line.split(" ").filter(function(word) {
    return word !== " " && word !== ""; //We don't care about the spaces and empty lines
  }).map(function(word) {
    return [word,1];                    //This will create the tuples ['Word', x]
  });

  return array //Finally we can return the result
}

//Reduce function
var reduce = function(array)
{
  //res will receive the reduced array
  var res = [];

  //For each tuple ["Word", x]
  for(var i=0;i<array.length;i++)
  {
    var k = 0;

    //We check if the word has been added before in the words we already have
    for(var j=0;j<res.length;j++)
    {
      //If it is true, we increment the value X of the tuple [Word, X]
      if(res[j][0] === array[i][0])
      {
        res[j][1] += array[i][1];

        //This is only to remember we do not need to add the word again to the result
        k=1;
      }
    }

    //If the word has not been added yet
    if(k === 0)
    {
      res.push(array[i]);
    }
  }

  return res;
}

//Output writer function
var outputWriter = function(array)
{
  for(var i=0;i<array.length;i++)
    console.log(array[i][0] + " : " + array[i][1]);
}

//This functiion tests everything in order
var test = function() {

  //We import the node File System library
  fs = require('fs')

  //We open the file lyrics.txt (try with lyrics2.txt for more fun)
  fs.readFile('lyrics.txt', 'utf8', function (err,data) {

    //Input reader splits the file into multiple lines and store them into an array
    var array = data.split("\n");

    //This array will receive each tuple ['Word', 1]
    var res = [];

    //For each line of the text
    for(var i=0;i<array.length;i++)
    {
      //We map the line and push the result in the 'res' array
      res.push.apply(res,map(array[i]));
    }

    //Then we can sort the array before the reduce
    res.sort();

    //Reducing...
    res = reduce(res);

    //Showing the output
    console.log(outputWriter(res));
  });
}

//Test launch
test();
