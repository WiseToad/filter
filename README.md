Примеры моего кода, 2024 год.  
Код только для демонстрации, частичный, к сборке не готов.

Микросервис фильтрации повторяющихся данных от контрагентов.

Назначение: Поток XML-файлов от контрагентов скачать с S3, распарсить, дедуплицировать и выявить 
непришедшие записи с момента прошлой обработки по содержимому значимых полей, отправить в Кафку для дальнейшей обработки.

Упор на большие объемы данных (десятки тысяч контрагентов ежедневно шлют файлы объемом в сотни мегабайт каждый).
