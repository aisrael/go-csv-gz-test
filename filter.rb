#!/usr/bin/env ruby

require "csv"
require "time"
require "zlib"

MINIMUM_TIME = Time.parse("2017-11-02T00:00:00.000Z")

# process_file processes the .csv.gz files as a stream of bytes counting all records that
# meet the minimum date
def process_file(filename)
  puts "Processing: #{filename}"
  total = 0
  matched = 0
  File.open(filename) do |file|
    Zlib::GzipReader.wrap(file) do |gzip|
      CSV.new(gzip).each do |row|
        time = Time.parse(row[3])
        matched += 1 if time > MINIMUM_TIME
        total += 1
      end
    end
  end

  {total: total, matched: matched}
end

START_TIME = Time.now

total = 0
matched = 0
Dir.glob("./testdata/*.csv.gz") do |filename|
  result = process_file(filename)
  total += result[:total]
  matched += result[:matched]
end

END_TIME   = Time.now
TOTAL_TIME = END_TIME - START_TIME
printf "Total: %d, Matched: %d, Ratio: %0.2f%%\n", total, matched, (matched.to_f*100.0/total.to_f)
puts "Time: #{TOTAL_TIME}"
