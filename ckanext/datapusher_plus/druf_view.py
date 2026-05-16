
# encoding: utf-8
from __future__ import annotations

import random

from flask import Blueprint
from flask.views import MethodView

from ckan import model
from ckan.plugins.toolkit import NotAuthorized, abort, h, get_action, _, request, g


resource_first = Blueprint('resource_first', __name__)


class _ResourceFirst(MethodView):
    def post(self):
        while True:
            idx = random.randrange(10**4, 10**5)
            name = f'dataset{idx}'
            if not model.Package.get(name):
                break
        data = {
            'name': name,
            'state': 'active',
        }
        if 'owner_org' in request.form:
            data['owner_org'] = request.form['owner_org']
        if 'type' in request.form:
            data['type'] = request.form['type']

        # Build a real auth context — empty {} bypasses CKAN's standard idiom
        # and makes auth fail on missing-user rather than wrong-user.
        ctx = {
            'model': model,
            'session': model.Session,
            'user': g.user,
            'auth_user_obj': getattr(g, 'userobj', None),
        }
        try:
            pkg = get_action('package_create')(ctx, data)
        except NotAuthorized:
            return abort(403, _('Unauthorized to create a package'))

        return h.redirect_to('dataset_resource.new', id=pkg['id'])


resource_first.add_url_rule(
    '/resource-first/new',
    view_func=_ResourceFirst.as_view('new'),
)


def get_blueprints():
    """Return blueprints for this view"""
    return [resource_first]