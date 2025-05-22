import sqlite3
import datetime

def calculate_all_features(conn, chart_type, chart_date):
    c = conn.cursor()
    table_name = f'{chart_type}_chart_entries'
    print(f'Processing {chart_type} chart for {chart_date}...')
    
    # Get all chart dates
    c.execute(f"SELECT DISTINCT chart_date FROM {table_name} ORDER BY chart_date")
    all_dates = [date[0] for date in c.fetchall()]
    
    # Find the index of the current date
    if chart_date not in all_dates:
        print(f'Date {chart_date} not found in {all_dates}')
        return
    current_date_index = all_dates.index(chart_date)
    
    # Get all entries for this date
    c.execute(f"SELECT title, artist, position, streams, release_date FROM {table_name} WHERE chart_date = ?", (chart_date,))
    current_entries = c.fetchall()
    
    for title, artist, position, streams, release_date in current_entries:
        engineered_data = {}
        
        # Calculate days since release
        if release_date:
            try:
                release_date_obj = datetime.datetime.strptime(release_date, '%Y-%m-%d')
                current_date_obj = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
                days_since = (current_date_obj - release_date_obj).days
                engineered_data['days_since_release'] = max(0, days_since)
                print(f"Days since release for {title}: {engineered_data['days_since_release']}")
            except Exception as e:
                print(f"Error calculating days since release: {e}")
        
        # Calculate weekend flag
        try:
            chart_date_obj = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
            if chart_date_obj.weekday() >= 5:  # 5=Saturday, 6=Sunday
                engineered_data['is_weekend'] = 1
                print(f"Weekend detected for {title}")
            else:
                engineered_data['is_weekend'] = 0
        except Exception as e:
            print(f'Error determining weekend: {e}')
        
        # Check if is a new entry
        c.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE title = ? AND artist = ? AND chart_date < ?
        """, (title, artist, chart_date))
        
        previous_appearances = c.fetchone()[0]
        if previous_appearances == 0:
            engineered_data['is_new_entry'] = 1
            print(f"New entry detected for {title}")
            
            # First entry info
            engineered_data['first_entry_date'] = chart_date
            engineered_data['first_entry_position'] = position
            engineered_data['total_days_on_chart'] = "1"
        else:
            engineered_data['is_new_entry'] = 0
            
            # Get earliest entry
            c.execute(f"""
                SELECT chart_date, position FROM {table_name} 
                WHERE title = ? AND artist = ? 
                ORDER BY chart_date ASC LIMIT 1
            """, (title, artist))
            
            first_entry = c.fetchone()
            if first_entry:
                engineered_data['first_entry_date'] = first_entry[0]
                engineered_data['first_entry_position'] = first_entry[1]
                
                # Calculate total days on chart
                c.execute(f"""
                    SELECT COUNT(DISTINCT chart_date) FROM {table_name} 
                    WHERE title = ? AND artist = ? AND chart_date <= ?
                """, (title, artist, chart_date))
                
                total_days = c.fetchone()[0]
                engineered_data['total_days_on_chart'] = str(total_days)
        
        # Get previous day's data if available
        if current_date_index > 0:
            previous_date = all_dates[current_date_index - 1]
            c.execute(f"SELECT position, streams FROM {table_name} WHERE title = ? AND artist = ? AND chart_date = ?", 
                     (title, artist, previous_date))
            prev_entry = c.fetchone()
            
            if prev_entry:
                prev_position, prev_streams = prev_entry
                
                # Set previous values
                engineered_data['prev_position'] = prev_position
                engineered_data['prev_streams'] = prev_streams
                print(f"Previous position for {title}: {prev_position}, Previous streams: {prev_streams}")
                
                # Calculate position change (positive = improved position)
                try:
                    current_position = int(position)
                    prev_position_int = int(prev_position)
                    # Positive means improved position (moved up the chart)
                    # e.g., from position 5 to position 3 would be +2
                    position_change = prev_position_int - current_position
                    engineered_data['position_change'] = str(position_change)
                    print(f"Position change for {title}: {position_change}")
                except Exception as e:
                    print(f'Error calculating position change: {e}')
                
                # Calculate streams change percentage
                try:
                    current_streams_int = int(streams.replace(',', '') if isinstance(streams, str) else streams)
                    prev_streams_int = int(prev_streams.replace(',', '') if isinstance(prev_streams, str) else prev_streams)
                    
                    if prev_streams_int > 0:
                        pct_change = ((current_streams_int - prev_streams_int) / prev_streams_int) * 100
                        engineered_data['streams_day_over_day_pct'] = f'{pct_change:.2f}'
                        print(f"Stream change % for {title}: {pct_change:.2f}%")
                except Exception as e:
                    print(f'Error calculating streams percentage: {e}')
        
        # Calculate 3-day rolling averages
        stream_values = []
        position_values = []
        
        # Add current entry
        try:
            current_streams_int = int(streams.replace(',', '') if isinstance(streams, str) else streams)
            current_position_int = int(position)
            stream_values.append(current_streams_int)
            position_values.append(current_position_int)
            
            # Add previous days
            for i in range(1, 3):  # Get up to 2 previous days
                if current_date_index - i >= 0:
                    prev_date = all_dates[current_date_index - i]
                    c.execute(f"SELECT position, streams FROM {table_name} WHERE title = ? AND artist = ? AND chart_date = ?",
                             (title, artist, prev_date))
                    prev_entry = c.fetchone()
                    
                    if prev_entry:
                        prev_pos, prev_str = prev_entry
                        try:
                            prev_streams_int = int(prev_str.replace(',', '') if isinstance(prev_str, str) else prev_str)
                            prev_position_int = int(prev_pos)
                            stream_values.append(prev_streams_int)
                            position_values.append(prev_position_int)
                        except Exception as e:
                            print(f'Error converting values: {e}')
            
            # Calculate averages if we have enough data
            if len(stream_values) > 0:
                avg_streams = sum(stream_values) / len(stream_values)
                avg_position = sum(position_values) / len(position_values)
                engineered_data['rolling_avg_streams_3day'] = f'{avg_streams:.2f}'
                engineered_data['rolling_avg_position_3day'] = f'{avg_position:.2f}'
                print(f"Rolling avg for {title} ({len(stream_values)} days): Streams={avg_streams:.2f}, Position={avg_position:.2f}")
        except Exception as e:
            print(f'Error calculating rolling averages: {e}')
        
        # Update the database with the engineered features
        if engineered_data:
            update_query = f"UPDATE {table_name} SET "
            update_parts = []
            params = []
            
            for key, value in engineered_data.items():
                update_parts.append(f"{key} = ?")
                params.append(value)
            
            update_query += ", ".join(update_parts)
            update_query += " WHERE chart_date = ? AND title = ? AND artist = ?"
            params.extend([chart_date, title, artist])
            
            c.execute(update_query, params)
            conn.commit()
            print(f'Updated {title} by {artist}')

def main():
    # Process all dates in the database
    conn = sqlite3.connect('spotify_charts.db')
    chart_types = ['global', 'usa']

    for chart_type in chart_types:
        c = conn.cursor()
        c.execute(f'SELECT DISTINCT chart_date FROM {chart_type}_chart_entries ORDER BY chart_date')
        dates = [date[0] for date in c.fetchall()]
        
        for chart_date in dates:
            calculate_all_features(conn, chart_type, chart_date)

    print('Done!')
    conn.close()

if __name__ == "__main__":
    main() 