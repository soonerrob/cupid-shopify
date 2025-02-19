import os
import subprocess

from flask import Flask, flash, redirect, render_template, request, url_for

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Replace with a secure secret key

# Default values for global variables
default_enable_tagging = True
default_smb_filename = "CupidWebSales.csv"
default_order_names = ""

def validate_order_names(order_names):
    """
    Validates the order names to ensure:
    - Each order starts with '#'.
    - Order numbers are separated by commas.
    - No trailing commas are present.
    """
    orders = [order.strip() for order in order_names.split(",") if order.strip()]
    
    # Check for invalid formats
    if not all(order.startswith("#") for order in orders):
        return False, "Each order number must start with a '#' sign."
    
    if not orders:
        return False, "Order names cannot be empty or improperly formatted."

    # Return validated orders as a comma-separated string
    return True, ", ".join(orders)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Get form inputs
        order_names = request.form.get("order_names", "").strip()
        smb_filename = request.form.get("smb_filename", "").strip()
        enable_tagging = request.form.get("enable_tagging") == "on"

        # Validate the input
        is_valid, message = validate_order_names(order_names)
        if not is_valid:
            # Preserve form state and show the error message
            flash(message, "danger")
            return render_template(
                "index.html",
                enable_tagging=enable_tagging,
                smb_filename=smb_filename,
                order_names=order_names,
            )

        # Call the standalone script as a subprocess
        try:
            env_vars = {
                **os.environ,
                "ORDER_NAMES": message,  # Use validated and formatted order names
                "SMB_FILENAME": smb_filename,
                "ENABLE_TAGGING": str(enable_tagging).lower(),
            }
            process = subprocess.run(
                ["python3", "export-invoicing-to-400-specified-gql.py"],
                env=env_vars,
                capture_output=True,
                text=True,
            )
            if process.returncode == 0:
                flash("Orders processed successfully.", "success")
            else:
                flash(f"Error: {process.stderr}", "danger")
        except Exception as e:
            flash(f"An error occurred: {str(e)}", "danger")

        return redirect(url_for("index"))

    # Pass default values for GET request
    return render_template(
        "index.html",
        enable_tagging=default_enable_tagging,
        smb_filename=default_smb_filename,
        order_names=default_order_names,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
