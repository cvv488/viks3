package main

import (
	"errors"
)

/*
Формат сообщений

LEN[2] без LEN и CRC | TID | DEST_ADDR[2] | SRC_ADDR[2] | BODY | CRC[2]
BODY_TID=0x80: | MSG_ID | OPTIONS: [Код опции, Длинна опции, Тело опции (от 0 до 255 байт)] ...  0xFF Конец опций

=> 00-2C=44 [80 00-00-00-00  20    50  11  (54-4D-44-52-56-20-76-2E-33-2E-39-2E-31-2E-31-33-39-)	51-02-(01-01-)	56-05-(41-64-6D-69-6E-)	57-05-(61-64-6D-69-6E-)	FF]	3E-EB
	Len     TId DAdr  SAdr   MsgId Opt Len  OptBody

2024-08-19 09:55:48.1511|TRACE|TcpSrv|VikingSrv [5555:0/1]  => [132] 00-80 80-00-00-00-00-
20-запрос на регистрацию (51-PointID 02 01-96) (56-Логин 08-70-75-31-6B-70-31-35-30) (57-00 пароль)
info 50 67 C0-CA-2D-35-30-30-20-52-54-4F-53-20-53-4E-3A-20-30-20-48-57-3A-20-31-2E-37-2E-30-20-46-57-3A-20-32-2E-35-2E-37-20-4D-4F-44-45-4D-3A-20-31-32-2E-30-30-2E-36-31-36-20-49-4D-45-49-3A-20-33-35-35-38-35-35-30-35-36-31-33-38-38-35-30-20-49-43-43-49-44-31-3A-20-38-39-37-30-31-30-32-38-33-34-38-31-30-39-36-34-39-32) FF-F7-1D
2024-08-19 09:55:48.1511|DEBUG|TcpSrv|VikingSrv [5555:0/1]  >ReqRegister_PointID:0196 ??-500 RTOS SN: 0 HW: 1.7.0 FW: 2.5.7 MODEM: 12.00.616 IMEI: 355855056138850 ICCID1: 897010283481096492
2024-08-19 09:55:48.1511|TRACE|TcpSrv|VikingSrv [5555:0/1]  <= [18] 00-0E-80-00-00-00-00-21-52-02-01-96-55-01-04-FF-52-F0
*/

const (
	// Тип сообщения
	TSLUG = 0x80 //служебное
	TINFO = 0x81 //информационное
	TSPOR = 0x82 //спорадическое

	//MSG_ID
	MID_QREG  = 0x20 //запрос на регистрацию
	MID_AREG  = 0x21 //ответ на запрос о регистрации
	MID_QSTAT = 0x22 //запрос статуса клиента
	MID_ASTAT = 0x23 //ответ на запрос статуса клиента
	MID_PING  = 0x28 //запрос “Keep alive”
	MID_PONG  = 0x29 //ответ на запрос “Keep alive”
	// 0x24 – уведомление о подключении/отключении клиента
	// 0x30 – подписка на уведомление о подключении/отключении клиента
	// 0x31 – ответ сервера на команду подписки
	// 0x32 – запрос статуса подписки
	MID_UNSU = 0xFE //команда не поддерживается

	//OPTIONS
	OPT_INF   = 0x50 //Информация о клиенте
	OPT_PID   = 0x51 //Идентификатор клиента (PointID)
	OPT_NETID = 0x52 //Сетевой идентификатор клиента (NetID)
	// 0x53 Список идентификаторов
	OPT_STAT = 0x55 //Статус клиента (len=1!)
	OPT_USER = 0x56 //Логин
	OPT_PASW = 0x57 //Пароль

	// 0x00 – неизвестный статус
	// 0х01 – гостевой доступ *
	// 0x02 – отключен
	// 0x03 – подключен *
	// 0x04 – аутентифицирован *
	// 0x05 – такой PointID уже занят
	// 0x06 – ошибка аутентификации
)

type VikingFrame struct {
	tid             byte //тип сообщения
	destadr, srcadr int  //uint16
	msgid           byte
	txb             []byte
	body            []byte
	// options []Option //[]byte
	// crc int //uint16
}

type Option struct {
	Code, len byte
	Body      []byte
}

func NewVikingFrame(typem byte, dest, src int, mid byte) *VikingFrame {
	hdr := make([]byte, 0, 1024)
	hdr = append(hdr, 0) //len
	hdr = append(hdr, 0)
	hdr = append(hdr, typem)
	hdr = append(hdr, BHL(dest)...)
	hdr = append(hdr, BHL(src)...)
	hdr = append(hdr, mid)
	return &VikingFrame{
		txb: hdr,
		tid: typem, destadr: dest, srcadr: src, msgid: mid, //buffer: make([]byte, 0, 1024),
	}
}

// завершает формирование служебного пакета: add 0xff, crc, set_Len
func (vf *VikingFrame) EndTx() {
	vf.txb = append(vf.txb, 0xff) //mark end opts
	lenp := len(vf.txb) - 2       //без [LEN]
	vf.txb[0] = byte(lenp >> 8)
	vf.txb[1] = byte(lenp)
	hi, lo := Crc(vf.txb)
	vf.txb = append(vf.txb, lo)
	vf.txb = append(vf.txb, hi)
}

// создает на основе пришедших байт
// test crc: ss := "00-0E-80-00-00-00-00-21-52-02-01-96-55-01-04-FF-52-F0"; bb, _ := HexToBuf(ss)
func NewVikingFrameRx(bb []byte) (*VikingFrame, error) {
	if len(bb) < 9 {
		return nil, errors.New("short_frx")
	}
	rxb := bb[:len(bb)-2] //без crc
	hi, lo := Crc(rxb)
	if hi != bb[len(bb)-1] || lo != bb[len(bb)-2] {
		return nil, errors.New("crc")
	}
	vf := VikingFrame{
		tid:     rxb[2],
		destadr: IHL(rxb[3:5]),
		srcadr:  IHL(rxb[5:7]),
	}
	if vf.tid == TSLUG {
		vf.msgid = rxb[7]
		vf.body = rxb[8:]
	} else {
		vf.body = rxb[7:]
	}
	return &vf, nil
}

func (vf *VikingFrame) AddOption(code byte, vv string) {
	vf.txb = append(vf.txb, code)
	vf.txb = append(vf.txb, byte(len(vv)))
	vf.txb = append(vf.txb, []byte(vv)...)
	// op := Option{code: code, len: byte(len(vv)), body: []byte(vv)}
	// vf.options = append(vf.options, op)
}
func (vf *VikingFrame) AddOptionInt(code byte, vv int) {
	vf.txb = append(vf.txb, code)
	vf.txb = append(vf.txb, 2)
	vf.txb = append(vf.txb, BHL(vv)...)
	// op := Option{code: code, len: 2, body: BHL(vv)}
	// vf.options = append(vf.options, op)
}
func (vf *VikingFrame) AddOptionByte(code byte, vv byte) {
	vf.txb = append(vf.txb, code)
	vf.txb = append(vf.txb, 1)
	vf.txb = append(vf.txb, vv)
}

func (vf *VikingFrame) GetOptions() map[int]Option {
	opts := map[int]Option{}
	pos := 0
	for pos < len(vf.body)-1 { //-1 0xff unused
		code := vf.body[pos]
		if code == 0xff {
			break
		}
		lenOpt := int(vf.body[pos+1])
		if pos+lenOpt+2 < len(vf.body) {
			// opts = append(opts, Option{code: code, body: vf.Rxb[pos+2 : pos+len+2]})
			opts[int(code)] = Option{Code: code, Body: vf.body[pos+2 : pos+lenOpt+2]}
		}
		pos += lenOpt + 2
	}
	return opts
}

// информационный пакет
func NewVikingFrameInf(dest, src int, data []byte) *VikingFrame {
	var txb []byte
	txb = append(txb, BHL(len(data)+5)...) //LEN
	txb = append(txb, TINFO)
	txb = append(txb, BHL(dest)...)
	txb = append(txb, BHL(src)...)
	txb = append(txb, data...)
	hi, lo := Crc(txb)
	txb = append(txb, lo)
	txb = append(txb, hi)
	return &VikingFrame{
		txb: txb,
	}
}

func Crc(bb []byte) (hi byte, lo byte) {
	if len(bb) > 0 {
		var data uint16
		var crc uint16 = 0xFFFF
		for _, v := range bb {
			data = uint16(v)
			for range 8 {
				if ((crc ^ data) & 1) != 0 {
					crc = (crc >> 1) ^ 0x8408
				} else {
					crc >>= 1
				}
				data >>= 1
			}
		}
		crc = ^crc
		hi = byte(crc >> 8)
		lo = byte(crc)
	}
	return
}
