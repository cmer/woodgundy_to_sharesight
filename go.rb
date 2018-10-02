VERSION = '0.6.0'

require 'roo'
require 'slop'
require 'stock_quote'
require 'csv'

require 'byebug'

MARKETS = {
  'CAD' => {
    default: 'TSE',
    'ETHC' => 'NEO'

  },
  'USD' => {
    'WFM' => 'NASDAQ',
    'ENB.PR.V' => 'TSE'
  }
}

ARGV << "--help" if ARGV.empty?

opts = Slop.parse do |o|
  o.separator ''
  o.separator 'Required:'

  o.string  '-i', '--input', "Path to input spreadsheet", required: true
  o.string  '-o', '--output', "Path to CSV output", required: true
  o.on '-v', '--version' do
    puts VERSION ; exit
  end

  o.on '-h', '--help' do
    puts o ; exit
  end
end

OUTPUT_DATE_FORMAT = '%d/%m/%Y'
OUTPUT_TYPES = %w(BUY SELL)

I = {
  account_number: 0,
  processing_date: 1,
  transaction_date: 2,
  ia_code: 3,
  type: 4,
  symbol: 5,
  quantity: 6,
  price: 7,
  currency: 8,
  total: 9,
  cash_balance: 10
}

@market_codes = {}

def get_field(row, input_field)
  pos  = I[input_field]
  cell = row[pos]

  if cell.type == :date
    cell.value.strftime(OUTPUT_DATE_FORMAT)
  elsif %i(quantity price).include?(input_field)
    cell.value.to_f
  else
    cell.value.to_s
  end
end

def skip_symbol?(sym)
  sym =~ /\d/
end

def market_code(row)
  symbol    = get_field(row, :symbol).upcase
  cur       = currency(row)
  cache_key = "#{symbol}/#{cur}"
  raise unless symbol && cur
  return @market_codes[cache_key] if @market_codes[cache_key]

  mc = case cur
  when 'CAD'
    MARKETS[cur][symbol] || MARKETS[cur][:default]
  when 'USD'
    MARKETS[cur][symbol] ||
    StockQuote::Stock.quote(symbol)&.primary_exchange ||
    (raise ArgumentError.new("Could not determine market code for #{symbol}"))
  else
    raise ArgumentError.new("Unknown currency #{cur}")
  end

  @market_codes[cache_key] = clean_up_market(mc)
end

def currency(row)
  acc = get_field(row, :account_number).upcase
  last_char = acc[-1]

  case last_char
  when 'U'
    'USD'
  when 'C'
    'CAD'
  else
    raise ArgumentError.new("Unknown currency: #{last_char}")
  end
end

def clean_up_market(market_name)
  if market_name =~ /New York Stock Exchange/i
    'NYSE'
  elsif market_name =~ /NASDAQ/i
    'NASDAQ'
  elsif market_name =~ /Toronto/i
    'TSE'
  else
    market_name
  end
end

output = []

puts "Opening #{opts[:input]}..."

xlsx = Roo::Spreadsheet.open(opts[:input])

OUTPUT_FIELDS = [ 'Trade Date',
                  'Instrument Code',
                  'Market Code',
                  'Quantity',
                  'Price in Dollars',
                  'Transaction Type',
                  'Brokerage',
                  'Brokerage Currency',
                  'Comments' ]

skipped = []

puts "Writing CSV to #{opts[:output]}..."

lines = []

xlsx.each_row_streaming(offset: 1) do |row|
  next unless OUTPUT_TYPES.include?(get_field(row, :type).upcase)
  sym = get_field(row, :symbol)
  txn_type = get_field(row, :type).upcase

  if skip_symbol?(sym)
    skipped << sym
    next
  end

  if txn_type == "BUY" && get_field(row, :quantity) < 0 ||
     txn_type == "SELL" && get_field(row, :quantity) > 0
    # reverse erronous trade line
    lines.reverse.each_with_index do |line, i|
      rev_qty = get_field(row, :quantity) * -1
      next unless line[1] == sym &&
                  line[3] == rev_qty &&
                  line[4] == get_field(row, :price).abs

      print 'X'
      lines.delete_at(i)
      break
    end
  else
    lines << [
      get_field(row, :transaction_date),
      sym,
      market_code(row),
      get_field(row, :quantity).abs,
      get_field(row, :price).abs,
      txn_type,
      '0',
      currency(row),
      ''
    ]
  end
  print '.'
end

lines.prepend(OUTPUT_FIELDS)
File.write(opts[:output], lines.map(&:to_csv).join)

if skipped.size > 0
  puts "\nSkipped: #{skipped.uniq.join(', ')}.\nDone!"
else
  puts "Done!"
end

