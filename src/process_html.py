import os
import re
from lxml import etree

def fix_mismatched_tags(input_file):
    """
    Before stripping tags process HTML to ensure no mismatched tags

    :param input_file: filename to be processed
    :type input_file: list[str]
    :return: Content with matched tags
    :rtype: list[str]

    """
    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    fixed_content = re.sub(r'(<link\b[^>]*?)(?<!/)>', 
                           r'\1 />', content)
    return fixed_content

def process_html(input_file, output_file):
    """
    Process html tags

    :param input_file: filename to be processed
    :type input_file: list[str]
    :param output_file: file to write
    :type output_file: list[str]
    :return: None
    :rtype: None

    """
    try:
        fixed_content = fix_mismatched_tags(input_file)
        root = etree.HTML(fixed_content)
        
        head = root.find("head")
        
        if head is not None:
            for element in list(head):
                elem_str = etree.tostring(
                            element, 
                            pretty_print = True).decode().strip()
                
                if element.tag == "link":
                    rel = element.attrib.get("rel")
                    if rel != "stylesheet":
                        head.remove(element)
                elif element.tag == "meta":
                    name = element.attrib.get("name")
                    if name != "cdc:last_published":
                        head.remove(element)
                else:
                    head.remove(element)
        else:
            print("Warning: No <head> tag found.")
        
        headers = root.findall(".//header")
        for header in headers:
            parent = header.getparent()
            if parent is not None:
                parent.remove(header)  

        alert_div = etree.Element("div", {
            "class": "alert",
            "style": "border:2px solid red; padding:10px; background-color:#ffecec;"
        })

        alert_div.text = (
            "Disclaimer: OriginalCDCMirror is not affiliated with, endorsed by, or "
            "connected to the U.S. Centers for Disease Control and Prevention (CDC) "
            "or any other government agency. This site does not represent itself as "
            "the CDC, nor does it provide official public health guidance or "
            "government-approved information. For official CDC resources, visit "
            "www.cdc.gov."
        )

        body = root.find(".//body")
        if body is not None:
            body.insert(0, alert_div)  
        else:
            print("No <body> tag found. Alert div not added.")

        for a_tag in root.findall(".//a"):
            href = a_tag.get("href")
            if href and "cdc.gov" in href:
                new_href = href.replace("cdc.gov", "OriginalCDCMirror.org")
                a_tag.set("href", new_href)

        with open(output_file, "wb") as f:
            f.write(etree.tostring(root, pretty_print = True, 
                                    method = "html", encoding = "UTF-8"))
        print(f"Processed: {input_file} -> {output_file}")
    
    except Exception as e:
        print(f"Error processing {input_file}: {e}")

def process_directory(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok = True)
    
    for filename in os.listdir(input_dir):
        input_file = os.path.join(input_dir, filename)
        if os.path.isfile(input_file) and filename.endswith(".html"):
            output_file = os.path.join(output_dir, filename)
            process_html(input_file, output_file)
