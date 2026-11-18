# ADR 0003: AWS Emulator como práctica opcional de APIs

- Estado: aceptada
- Fecha: 2026-11-18

## Contexto

El proyecto debe poder ejecutarse de punta a punta sin una cuenta AWS, credenciales reales ni servicios facturables. Aun así, interesa practicar el uso de un SDK compatible con APIs de AWS en local.

## Decisión

Mantener AWS Emulator como una etapa aislada y opcional. La ruta principal PostgreSQL → Debezium → Kafka → Spark → Delta no dependerá del emulador.

En Windows el emulador se ejecuta en Docker. AWS no publica una imagen preconstruida: la imagen local se construye desde Amazon Linux e instala el paquete Linux oficial. El MVP fija AWS Emulator `1.0.513` como build argument reproducible, actualizable en `docker-compose.yml` tras revisar las notas de versión y fidelidad. El endpoint local usa HTTPS en `127.0.0.1:9297`; el contenedor debe publicar el puerto solo en loopback. Las pruebas usarán credenciales generadas por el emulador y el certificado CA que este crea localmente. No se guardarán esas credenciales en Git.

Como integración opcional, un proceso pequeño de Python escribe un objeto de prueba en un bucket S3 emulado, lo lee, lista y limpia. Esto ilustra una API del ecosistema AWS sin cambiar el almacenamiento Delta local ni afirmar que Spark corre en S3. La prueba local valida el flujo concreto; la fidelidad de otras operaciones o casos límite requiere consultar los reportes por versión y no se infiere paridad general.

La prueba del proyecto se ejecutó correctamente en AWS Emulator `1.0.513` con Boto3: `CreateBucket`, `PutObject`, `GetObject`, `ListObjectsV2`, `DeleteObject` y `DeleteBucket` terminaron en `PASS`. Este resultado demuestra que el flujo probado funciona en esta versión local; no demuestra equivalencia completa con AWS ni cubre otras operaciones o condiciones. AWS publica scores por servicio y advierte que la fidelidad varía según servicio y funcionalidad.

La telemetría del emulador está activada por defecto. La configuración del proyecto la desactiva mediante `EMULATOR_TELEMETRY=off`. El estado de los servicios emulados vive en memoria y se pierde al detener el proceso; no se tratará como almacenamiento duradero.

## Consecuencias

- No se necesita crear una cuenta AWS para el MVP ni para esta práctica local.
- Construir la imagen requiere descargar el paquete oficial del emulador y reservar espacio local para Docker.
- El emulador no cobra una tarifa de servicio, pero tampoco sustituye la verificación de límites, precios o comportamiento de AWS real.
- Las credenciales generadas son temporales y solo sirven para la ejecución local.
- Ninguna petición de esta etapa debe dirigirse a una cuenta AWS real.

## Referencias oficiales

- [Repositorio oficial AWS Emulator](https://github.com/aws/aws-emulator)
- [Ejecución en Docker para Windows y macOS](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-docker.html)
- [Versionado y paquete de una versión específica](https://docs.aws.amazon.com/emulator/latest/userguide/versioning.html)
- [Configuración de AWS SDK para el endpoint local](https://docs.aws.amazon.com/emulator/latest/userguide/point-sdk-to-emulator.html)
- [Qué es AWS Emulator y cuándo usarlo](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html)
- [Fidelidad y puntuaciones por versión](https://docs.aws.amazon.com/emulator/latest/userguide/fidelity.html)
- [Suite oficial de pruebas de fidelidad](https://github.com/aws/aws-emulator-evaluation-suite)
