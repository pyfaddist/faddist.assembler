from faddist.reflection import load_class, create_instance


def test_load_class():
    clazz = load_class('string.Template')
    from string import Template
    assert clazz is Template


def test_create_instance():
    clazz = load_class('string.Template')

    template = create_instance(clazz, {'template': 'replaced $template_var with reflect loaded class'})
    assert template.substitute(
        template_var='template variable') == 'replaced template variable with reflect loaded class'

    template = create_instance(clazz, ['replaced $template_var with reflect loaded class'])
    assert template.substitute(
        template_var='template variable') == 'replaced template variable with reflect loaded class'
