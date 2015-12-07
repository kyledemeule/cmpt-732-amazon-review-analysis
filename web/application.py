from flask import Flask, render_template, request, redirect, url_for, abort
from model import Model
app = Flask(__name__)

REVIEW_PAGE_LENGTH = 10

@app.route("/")
def index():
    top_reviewers = Model.get_top_reviewers(length=REVIEW_PAGE_LENGTH)
    top_products = Model.get_top_products(length=REVIEW_PAGE_LENGTH)
    return render_template('index.html', top_reviewers=top_reviewers, top_products=top_products)

@app.route("/search")
def search():
    search_type = request.args.get('search_type')
    search_term = request.args.get('search_term')
    if search_type == "product":
        return redirect(url_for('product', asin=search_term))
    else:
        # include a message
        redirect(url_for('index'))

@app.route('/product/<asin>')
def product(asin):
    product = Model.get_product(asin)
    reviews = Model.get_top_reviews(asin, length=REVIEW_PAGE_LENGTH)
    return render_template('product.html', product=product, reviews=reviews)

@app.route('/reviewer/<reviewer_id>')
def reviewer(reviewer_id):
    page = int(request.args.get('page')) if type(request.args.get('page')) is unicode else 0
    reviewer = Model.get_reviewer(reviewer_id)
    reviews = Model.get_reviews(reviewer=reviewer_id, page=page, length=REVIEW_PAGE_LENGTH)
    return render_template('reviewer.html', reviewer=reviewer, reviews=reviews)

@app.route('/review/<review_id>')
def review(review_id):
    review = Model.get_review(review_id)
    return render_template('review.html', review=review)

if __name__ == "__main__":
    app.run(debug=True)