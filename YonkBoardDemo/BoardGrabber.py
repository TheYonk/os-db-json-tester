import evdev
from evdev import InputDevice, categorize, ecodes
import asyncio



dev = evdev.InputDevice('/dev/input/event15')

#async def helper(device):
#    async for ev in device.async_read_loop():
#        print(repr(ev))
        
#loop = asyncio.get_event_loop()
#loop.run_until_complete(helper(device))


for event in dev.read_loop():
    if event.type == ecodes.EV_KEY:
       print(str(event))
       print(categorize(event))
       if event.value == 0 :
            if event.code == 288 :
                fast_trans = open(r"fast.bench", "w+")
                fast_trans.write('On')
                fast_trans.close()
            if event.code == 289 :
               fast_trans = open(r"aggre.bench", "w+")
               fast_trans.write('On')
               fast_trans.close()
            if event.code == 290 :
               fast_trans = open(r"adhoc.bench", "w+")
               fast_trans.write('On')
               fast_trans.close()
            
       if event.value == 1 :
            if event.code == 288 :
                fast_trans = open(r"fast.bench", "w+")
                fast_trans.write('off')
                fast_trans.close()
            if event.code == 289 :
                fast_trans = open(r"aggre.bench", "w+")
                fast_trans.write('off')
                fast_trans.close()
            if event.code == 290 :
                fast_trans = open(r"adhoc.bench", "w+")
                fast_trans.write('off')
                fast_trans.close()
