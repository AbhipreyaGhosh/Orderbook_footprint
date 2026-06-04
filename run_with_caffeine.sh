#!/bin/bash

# Caffeinate the Mac (prevent sleep) while running the Orderbook signal code
caffeinate -i python3 /Users/abhipreyaghosh/Desktop/Orderbook_footprint/Orderbook_signal.py
caffeinate -i python3 /Users/abhipreyaghosh/Desktop/Orderbook_footprint/Orderbook_signalV2.py