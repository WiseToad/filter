package ru.sozvezdie.filter.service

import org.springframework.beans.factory.BeanFactory
import org.springframework.stereotype.Service
import org.springframework.util.StringUtils
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.filter.AbstractFilter

@Service
class FilterProvider(
    private val applicationProperties: ApplicationProperties,
    private val beanFactory: BeanFactory
) {
    fun getObject(): AbstractFilter = beanFactory.getBean(StringUtils.uncapitalize(applicationProperties.filterType), AbstractFilter::class.java)
}
