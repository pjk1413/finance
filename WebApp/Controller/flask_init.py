from flask_cors import CORS
from flask import Flask

def create_app():
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__, instance_relative_config=True)
    CORS(app, resources={r'/*': {'origins': '*'}})
    app.config.from_mapping(
        # a default secret that should be overridden by instance config
        SECRET_KEY="dev",
    )

    from WebApp.Controller import auth, blog, login, retrieve_technical, retrieve_sentiment, schedule

    app.register_blueprint(login.bp)
    app.register_blueprint(retrieve_technical.bp)
    app.register_blueprint(retrieve_sentiment.bp)
    app.register_blueprint(schedule.bp)

    # make url_for('index') == url_for('blog.index')
    # in another app, you might define a separate main index here with
    # app.route, while giving the blog blueprint a url_prefix, but for
    # the tutorial the blog will be the main index
    app.add_url_rule("/", endpoint="index")

    return app
