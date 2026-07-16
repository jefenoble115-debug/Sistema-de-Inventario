-- 7. Función para el Trigger que automatiza el Stock
CREATE OR REPLACE FUNCTION actualizar_stock()
RETURNS TRIGGER AS $$
DECLARE
    v_activo BOOLEAN;
    v_stock_actual INT;
BEGIN
    -- Obtener el estado actual y el stock del producto
    SELECT activo, stock_sistema INTO v_activo, v_stock_actual 
    FROM productos 
    WHERE id_producto = NEW.id_producto;

    -- VALIDACIÓN: Si el producto está deshabilitado, no se permiten movimientos
    IF v_activo = FALSE THEN
        RAISE EXCEPTION 'No se pueden realizar movimientos en el producto ID % porque está deshabilitado.', NEW.id_producto;
    END IF;

    -- Lógica de actualización de stock
    IF NEW.tipo_movimiento = 'ENTRADA' THEN
        UPDATE productos 
        SET stock_sistema = stock_sistema + NEW.cantidad
        WHERE id_producto = NEW.id_producto;
        
    ELSIF NEW.tipo_movimiento = 'SALIDA' THEN
        IF v_stock_actual < NEW.cantidad THEN
            RAISE EXCEPTION 'Stock insuficiente para realizar la salida del producto ID %', NEW.id_producto;
        END IF;
        
        UPDATE productos 
        SET stock_sistema = stock_sistema - NEW.cantidad
        WHERE id_producto = NEW.id_producto;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 8. Crear el Trigger para actualizar el stock
CREATE OR REPLACE TRIGGER trigger_movimientos

  
AFTER INSERT ON movimientos
FOR EACH ROW
EXECUTE FUNCTION actualizar_stock();
