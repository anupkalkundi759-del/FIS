# Factory Intelligence System

## Overview
This project is a production planning and tracking system designed to manage factory workflow across multiple houses and products.

It integrates:
- Tracking system (actual progress)
- Scheduling engine (P6-style prediction)
- Capacity-based workflow simulation

---

## Features

### 1. Excel Upload
- Upload project, unit, house, and product data

### 2. Scheduling Engine
- Simulates workflow:
  Measurement → Cutting List → Production → Pre Assembly → Polishing → Final Assembly → Dispatch
- Generates predicted finish dates

### 3. Dashboard
- Total houses
- Predicted houses
- Delayed houses

### 4. Tracking
- View house-level progress

### 5. Product Tracking
- Filter by project, unit, house, product

---

## Workflow

1. Upload Excel
2. Update Measurement Date
3. Run Scheduling Engine
4. Monitor Dashboard

---

## Tech Stack
- Python (Streamlit)
- PostgreSQL / Supabase

---

## Future Improvements
- Queue-based scheduling
- Bottleneck detection
- Product-level duration logic
- Delay prediction
