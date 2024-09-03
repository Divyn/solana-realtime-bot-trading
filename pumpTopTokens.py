import asyncio
import json
import logging
import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.error import RetryAfter
from html import escape

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Your bot token from the BotFather
BOT_TOKEN = '75019...'

# Function to split long text into smaller parts
def split_text(text, max_length):
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

# Function to send a long message as multiple smaller messages
async def send_long_message(update: Update, context: ContextTypes.DEFAULT_TYPE, long_message, max_message_length=4000):
    message_parts = split_text(long_message, max_message_length)
    for part in message_parts:
        while True:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id, 
                    text=part,
                    parse_mode=ParseMode.HTML
                )
                break  # Break the loop if the message is sent successfully
            except RetryAfter as e:
                logging.warning(f"Flood control exceeded. Retrying in {e.retry_after} seconds.")
                await asyncio.sleep(e.retry_after)  # Wait for the specified time before retrying

# Function to send the query and process the response
async def send_query_and_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = 'https://streaming.bitquery.io/eap'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ory_at_...E'
    }
    query = """
    query {
      Solana {
        DEXTradeByTokens(
          orderBy: {descendingByField: "buy"}
          where: {Trade: {Currency: {MintAddress: {notIn: ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]}}, Dex: {ProtocolFamily: {is: "Pumpfun"}}}, Transaction: {Result: {Success: true}}}
          limit: {count: 5}
        ) {
          Trade {
            Currency {
              Symbol
              Name
              MintAddress
            }
            Side{
              Currency{
                Symbol
                MintAddress
              }
            }
          }
          buy: sum(of: Trade_Side_AmountInUSD, if: {Trade: {Side: {Type: {is: buy}}}})
          sell: sum(of: Trade_Side_AmountInUSD, if: {Trade: {Side: {Type: {is: sell}}}})
        }
      }
    }
    """
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json={'query': query}) as response:
            response_text = await response.text()
            response_json = json.loads(response_text)
            solana_data = response_json.get('data', {}).get('Solana', {}).get('DEXTradeByTokens', [])
            formatted_message = format_message(solana_data)
            await send_long_message(update, context, formatted_message)

def format_message(data):
    message = ""
    for item in data:
        try:
            # Escape dynamic content to prevent issues with HTML parsing
            symbol = escape(item['Trade']['Currency'].get('Symbol', 'N/A'))
            name = escape(item['Trade']['Currency'].get('Name', 'N/A'))
            mint_address = escape(item['Trade']['Currency'].get('MintAddress', 'N/A'))
            buy = escape(str(item.get('buy', 'N/A')))
            sell = escape(str(item.get('sell', 'N/A')))
            side_mint_address = escape(item['Trade']['Side']['Currency'].get('MintAddress', 'N/A'))

            message_part = (
                f"<b>{symbol} ({name})</b>\n"
                f"<code>Address: {mint_address}</code>\n"
                f"💸 <b>Buy Amount in USD:</b> {buy}\n"
                f"💰 <b>Sell Amount in USD:</b> {sell}\n\n"
                f"🔗 <a href='https://dexrabbit.com/solana/pumpfun/{mint_address}'>Trade Now</a>\n\n"
            )

            # Check if adding this message part will exceed Telegram's message length limit
            if len(message) + len(message_part) > 4096:
                logging.warning("Message length exceeded, consider sending the message in parts.")
                break  # Stop adding more content to avoid exceeding limits

            message += message_part
            
        except Exception as e:
            logging.error(f"Error formatting message for item: {item}. Error: {str(e)}")
            continue  # Skip this item if there's an error in formatting
    
    # Ensure all tags are closed properly
    if message.count('<code>') != message.count('</code>'):
        logging.error("Mismatched <code> tags detected")
        message = message.replace('<code>', '').replace('</code>', '')  # Quick fix by removing code tags

    return message

# Function to start the regular requests
async def start_regular_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    while True:
        await send_query_and_process(update, context)
        await asyncio.sleep(120)  # Wait for 2 minutes before sending the next request

# Command handler to start the regular requests
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Starting regular requests every 2 minutes...")
    asyncio.create_task(start_regular_requests(update, context))

# Main function to set up the Telegram bot
if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    start_handler = CommandHandler('startTopPumpTokens', start)
    application.add_handler(start_handler)

    application.run_polling()
