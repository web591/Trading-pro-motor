-- 🔍 VERIFICACIÓN EXTRA DE SEGURIDAD CONTABLE POST-PRUEBA
SELECT broker, tipo_evento, COUNT(*), MIN(fecha_utc) as desde, MAX(fecha_utc) as hasta
FROM sys_cashflows
WHERE user_id = 1
GROUP BY broker, tipo_evento;