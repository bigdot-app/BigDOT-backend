module.exports = {
  print: function() {
    process.stdout.write(Object.values(arguments).join(' '));
  },
  println: function() {
    process.stdout.write(Object.values(arguments).join(' ') + '\n');
  },
  secondsEpoch: function() {
    return Math.floor(Date.now() / 1000);
  },
  toSeconds: require('sec')
}