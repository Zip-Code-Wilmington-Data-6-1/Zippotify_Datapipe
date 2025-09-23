#!/usr/bin/env python3
"""
Create a neon-themed star schema data model image for the TracktionAI dashboard
"""

from PIL import Image, ImageDraw, ImageFont
import os

def create_neon_data_model_image():
    """Create a modified star schema image with dashboard's neon color palette"""
    
    # Dashboard neon colors
    NEON_COLORS = {
        'background': '#0e1117',  # Dashboard dark background
        'primary_key': '#FFFF00',  # Electric Yellow (for PK)
        'foreign_key': '#FF6600',  # Electric Orange (for FK)
        'fact_table': '#8000FF',   # Electric Violet (for fact table)
        'dim_table': '#00FFFF',    # Electric Cyan (for dimension tables)
        'text': '#fafafa',         # White text
        'border': '#404040'        # Gray borders
    }
    
    # Create new image with dark background
    img_width, img_height = 1200, 800
    img = Image.new('RGB', (img_width, img_height), NEON_COLORS['background'])
    draw = ImageDraw.Draw(img)
    
    # Title
    title_font_size = 36
    try:
        title_font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", title_font_size)
        text_font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", 14)
        small_font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", 12)
    except:
        title_font = ImageFont.load_default()
        text_font = ImageFont.load_default()
        small_font = ImageFont.load_default()
    
    # Draw title
    title_text = "Music Analytics - Star Schema Data Model"
    title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((img_width - title_width) // 2, 30), title_text, 
              fill=NEON_COLORS['text'], font=title_font)
    
    # Define table positions and sizes
    table_width, table_height = 200, 180
    
    # Dimension tables (top row)
    dim_tables = [
        {"name": "dim_user", "x": 80, "y": 120, "fields": [
            ("user_id: int (PK)", "pk"),
            ("age_range: string", "normal"),
            ("gender: string", "normal"),
            ("registration_dt", "normal"),
            ("birthday: ts", "normal"),
            ("location_id", "normal")
        ]},
        {"name": "dim_song", "x": 340, "y": 120, "fields": [
            ("song_id: int (PK)", "pk"),
            ("song_title: string", "normal"),
            ("artist_id: int", "normal"),
            ("genre_id: int", "normal"),
            ("album_id: trans", "normal"),
            ("release_date: date", "normal"),
            ("duration: int", "normal")
        ]},
        {"name": "dim_time", "x": 600, "y": 120, "fields": [
            ("time_key: int (PK)", "pk"),
            ("date: date", "normal"),
            ("hour: int", "normal"),
            ("weekday: string", "normal"),
            ("month: int", "normal"),
            ("year: int", "normal")
        ]},
        {"name": "dim_genre", "x": 860, "y": 120, "fields": [
            ("genre_id: int (PK)", "pk"),
            ("genre_name: string", "normal"),
            ("genre_category: string", "normal")
        ]}
    ]
    
    # Fact table (center)
    fact_table = {
        "name": "fact_plays", "x": 420, "y": 350, "width": 300, "height": 140,
        "fields": [
            ("play_id: int (PK)", "pk"),
            ("user_id: int", "fk"),
            ("song_id: int", "fk"),
            ("artist_id: int", "fk"),
            ("genre_id: int", "fk"),
            ("location_id: int", "fk"),
            ("time_key: int", "fk")
        ]
    }
    
    # Bottom dimension tables
    bottom_tables = [
        {"name": "dim_artist", "x": 180, "y": 580, "fields": [
            ("artist_id: int (PK)", "pk"),
            ("artist_name: string", "normal")
        ]},
        {"name": "dim_location", "x": 640, "y": 580, "fields": [
            ("location_id: int (PK)", "pk"),
            ("city: string", "normal"),
            ("state: string", "normal"),
            ("lat_dec", "normal"),
            ("long_dec", "normal")
        ]}
    ]
    
    def draw_table(table_info, is_fact=False):
        x, y = table_info["x"], table_info["y"]
        width = table_info.get("width", table_width)
        height = table_info.get("height", table_height)
        
        # Table background color
        bg_color = NEON_COLORS['fact_table'] if is_fact else NEON_COLORS['dim_table']
        
        # Draw table rectangle with gradient effect
        draw.rectangle([x, y, x + width, y + height], 
                      fill=bg_color, outline=NEON_COLORS['border'], width=2)
        
        # Table name header
        header_height = 30
        draw.rectangle([x, y, x + width, y + header_height], 
                      fill=bg_color, outline=NEON_COLORS['border'], width=2)
        
        # Table name text
        name_bbox = draw.textbbox((0, 0), table_info["name"], font=text_font)
        name_width = name_bbox[2] - name_bbox[0]
        draw.text((x + (width - name_width) // 2, y + 8), table_info["name"], 
                 fill=NEON_COLORS['text'], font=text_font)
        
        # Fields
        field_y = y + header_height + 10
        for field, field_type in table_info["fields"]:
            if field_type == "pk":
                color = NEON_COLORS['primary_key']
            elif field_type == "fk":
                color = NEON_COLORS['foreign_key']
            else:
                color = NEON_COLORS['text']
            
            draw.text((x + 10, field_y), field, fill=color, font=small_font)
            field_y += 18
    
    # Draw all dimension tables
    for table in dim_tables + bottom_tables:
        draw_table(table)
    
    # Draw fact table
    draw_table(fact_table, is_fact=True)
    
    # Draw relationship lines
    def draw_connection(from_table, to_table, color=NEON_COLORS['foreign_key']):
        from_x = from_table["x"] + table_width // 2
        from_y = from_table["y"] + table_height
        to_x = to_table["x"] + (to_table.get("width", table_width)) // 2
        to_y = to_table["y"]
        
        draw.line([from_x, from_y, to_x, to_y], fill=color, width=3)
    
    # Connect dimension tables to fact table
    for table in dim_tables:
        if table["y"] < fact_table["y"]:  # Top tables
            from_x = table["x"] + table_width // 2
            from_y = table["y"] + table_height
            to_x = fact_table["x"] + fact_table["width"] // 2
            to_y = fact_table["y"]
            draw.line([from_x, from_y, to_x, to_y], fill=NEON_COLORS['foreign_key'], width=3)
    
    for table in bottom_tables:
        from_x = table["x"] + table_width // 2
        from_y = table["y"]
        to_x = fact_table["x"] + fact_table["width"] // 2
        to_y = fact_table["y"] + fact_table["height"]
        draw.line([from_x, from_y, to_x, to_y], fill=NEON_COLORS['foreign_key'], width=3)
    
    # Add legend
    legend_x, legend_y = 80, img_height - 120
    legend_items = [
        ("🟡 Primary Key (PK)", NEON_COLORS['primary_key']),
        ("🟠 Foreign Key (FK)", NEON_COLORS['foreign_key']),
        ("🟣 Fact Table", NEON_COLORS['fact_table']),
        ("🔵 Dimension Table", NEON_COLORS['dim_table'])
    ]
    
    draw.text((legend_x, legend_y - 20), "Legend:", fill=NEON_COLORS['text'], font=text_font)
    for i, (label, color) in enumerate(legend_items):
        y_pos = legend_y + i * 20
        draw.rectangle([legend_x, y_pos, legend_x + 15, y_pos + 15], fill=color)
        draw.text((legend_x + 25, y_pos), label, fill=NEON_COLORS['text'], font=small_font)
    
    # Save the image
    output_path = "StarSchemaDataModel.png"
    img.save(output_path)
    print(f"✅ Created neon-themed data model image: {output_path}")
    
    return output_path

if __name__ == "__main__":
    create_neon_data_model_image()
