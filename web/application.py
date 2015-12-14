from flask import Flask, render_template, request, redirect, url_for, abort
from model import Model
application = Flask(__name__)

REVIEW_PAGE_LENGTH = 10

@application.route("/")
def index():
    return render_template('index.html')

@application.route("/search")
def search():
    search_type = request.args.get('search_type').strip()
    search_term = request.args.get('search_term').strip()
    if search_type == "product":
        return redirect(url_for('product', asin=search_term))
    else:
        # include a message
        redirect(url_for('index'))

@application.route('/product/<asin>')
def product(asin):
    product = Model.get_product(asin)
    if not product:
        abort(404)
    reviews = Model.get_top_reviews(asin, length=REVIEW_PAGE_LENGTH)
    return render_template('product.html', product=product, reviews=reviews)

@application.route('/reviewer/<reviewer_id>')
def reviewer(reviewer_id):
    reviewer = Model.get_reviewer(reviewer_id)
    if not reviewer:
        abort(404)
    reviews = Model.get_reviews(reviewer_id=reviewer_id, length=REVIEW_PAGE_LENGTH)
    return render_template('reviewer.html', reviewer=reviewer, reviews=reviews)

@application.route('/review/<reviewer_id>/<asin>')
def review(reviewer_id, asin):
    review = Model.get_review(reviewer_id, asin)
    if not review:
        abort(404)
    return render_template('review.html', review=review)

if __name__ == "__main__":
    application.run(debug=True)